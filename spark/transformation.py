from pyspark.sql import SparkSession
from pyspark.sql.functions import \
    udf, col, regexp_replace, split, array, \
    when, slice, to_date, arrays_zip, sort_array, \
    explode, collect_list, count, size, create_map, \
    lit, array_contains, coalesce, size, when, \
    concat, length
from pyspark.sql.functions import lower as lower_
from pyspark.sql.types import ArrayType, StringType, IntegerType, MapType, DoubleType
from pyspark.ml.feature import Tokenizer, StopWordsRemover, CountVectorizer, IDF
from pyspark.ml import Pipeline
from itertools import chain
import re
import html
import os


def read_tags_raw(tags_string):
    """converts <tag1><tag2> to ['tag1', 'tag2']"""
    return html.unescape(tags_string).strip('>').strip('<').split('><') if tags_string else []


def read_tags():
    return udf(read_tags_raw, ArrayType(StringType()))


def parse_line(line):
    regex = re.compile(' ([A-Za-z]+)="([^"]*)"')
    return {key: value for key, value in regex.findall(line)}


def unescape():
    return udf(lambda text: html.unescape(text) if text else None)


def blank_as_null(x):
    """Replace Null type strings with an empty string. Useful because some SparkML transformers
    choke on Null values. """
    return when(col(x) != "", col(x)).otherwise("")


def convert_posts(spark, link, clean_text=True):
    """Reads in raw XML file of stackoverflow posts from a S3 bucket link
    and processes the XML into a spark dataframe.
    """
    parsed = spark.read.text(link).where(col('value').like('%<row Id%')) \
        .select(udf(parse_line, MapType(StringType(), StringType()))('value').alias('value')) \
        .select(
        col('value.Id').cast('integer'),
        col('value.PostTypeId').cast('integer'),
        col('value.ParentId').cast('integer'),
        col('value.AcceptedAnswerId').cast('integer'),
        col('value.CreationDate').cast('timestamp'),
        col('value.Score').cast('integer'),
        col('value.ViewCount').cast('integer'),
        unescape()('value.Body').alias('Body'),
        col('value.OwnerUserId').cast('integer'),
        col('value.LastEditorUserId').cast('integer'),
        col('value.LastEditorDisplayName'),
        col('value.LastEditDate').cast('timestamp'),
        col('value.LastActivityDate').cast('timestamp'),
        col('value.CommunityOwnedDate').cast('timestamp'),
        col('value.ClosedDate').cast('timestamp'),
        unescape()('value.Title').alias('Title'),
        read_tags()('value.Tags').alias('Tags'),
        col('value.AnswerCount').cast('integer'),
        col('value.CommentCount').cast('integer'),
        col('value.FavoriteCount').cast('integer'))
    if not clean_text:
        return parsed
    else:
        return process_text(parsed)


def convert_tags(spark, link):
    """Reads in raw XML file of stackoverflow tags from a S3 bucket link
    and processes the XML into a spark dataframe.
    """
    return spark.read.text(link).where(col('value').like('%<row Id%')) \
        .select(udf(parse_line, MapType(StringType(), StringType()))('value').alias('value')) \
        .select(
        col('value.Id').cast('integer'),
        col('value.TagName'),
        col('value.Count').cast('integer'),
        col('value.ExcerptPostId').cast('integer'),
        col('value.WikiPostId').cast('integer')
    )


def extract_top_tags(converted_tags, top=100):
    """Finds the most commonly used tags. Useful for shrinking universe of tags to consider."""
    top = converted_tags.orderBy("Count", ascending=False).limit(top)
    return top.select("TagName").rdd.flatMap(lambda x: x).collect()


def tag_transfer(posts):
    """Creates a new column Tags_All, which, when a post is an answer,
    fills in the tags for the corresponding question. Needed to associate tags to each type of post."""
    answers = posts.where(col("ParentId").isNull())
    answers = answers.withColumnRenamed('Tags', 'Tags_Question')
    answers = answers.withColumnRenamed('Id', 'Id_Answer')
    answers = answers.withColumnRenamed('ParentId', 'ParentId_Answer')
    answers = answers['Tags_Question', 'Id_Answer', 'ParentId_Answer']
    left_join = posts.join(answers, posts.ParentId == answers.Id_Answer, how='left_outer')
    left_join = left_join.withColumn('Tags_All', coalesce(left_join.Tags_Question, left_join.Tags))
    return left_join


def select_with_tag(posts, tag):
    return posts.where(array_contains('Tags_All', tag))


def process_text(parsed_dataframe):
    """Given a dataframe of parsed Stackoverflow posts, performs basic text cleaning operations
     on each textual column. Also combines Title and Body into a single column Text_clean."""

    def clean_string(s):
        """Performs the following in PySpark: makes string lowercase, removes whitespace characters,
        removes markup tags, and removes punctuation."""
        s = lower_(s)
        s = regexp_replace(s, "\n", "")
        s = regexp_replace(s, "<[^>]*>", "")  # remove markup tags
        s = regexp_replace(s, "[^\\w\\s]", "")  # remove punctuation
        s = regexp_replace(s, "\b\\w{1,2}\b", "")  # remove small words
        return s

    cleaned = parsed_dataframe.withColumn("Body", clean_string(parsed_dataframe['Body']))
    cleaned = cleaned.withColumn("Title", clean_string(parsed_dataframe['Title']))
    cleaned = cleaned.withColumn("Body", blank_as_null("Body"))
    cleaned = cleaned.withColumn("Title", blank_as_null("Title"))
    cleaned = cleaned.withColumn('Text',
                                 concat(col('Body'), lit(' '), col('Title')))
    cleaned = cleaned.withColumn("CreationDate", to_date(col("CreationDate")))

    return cleaned


def body_pipeline(cleaned_dataframe):
    """NLP pipeline. Tokenizes, removes stopwords, and computes TF-IDF
    Returns transformed data as 'features' and the vocabulary of words."""

    tokenizer = Tokenizer(inputCol="Text", outputCol="Text_tokens")
    stop_remover = StopWordsRemover(inputCol=tokenizer.getOutputCol(), outputCol="Text_tokens_stopped")
    count_vect = CountVectorizer(inputCol=stop_remover.getOutputCol(), outputCol="Text_counts_raw")
    idf = IDF(inputCol=count_vect.getOutputCol(), outputCol="features")

    pipeline = Pipeline(stages=[tokenizer, stop_remover, count_vect, idf])
    model = pipeline.fit(cleaned_dataframe)
    featurized_data = model.transform(cleaned_dataframe)

    return featurized_data, model.stages[-2].vocabulary


def extract_top_keywords(posts, n_keywords=10):
    """Given TF-IDF output (as "features" column) extracts out the vocabulary index of the
    10 keywords with highest TF-IDF (for each post)."""

    def extract_keys_from_vector(vector):
        return vector.indices.tolist()

    def extract_values_from_vector(vector):
        return vector.values.tolist()

    extract_keys_from_vector_udf = udf(lambda vector: extract_keys_from_vector(vector), ArrayType(IntegerType()))
    extract_values_from_vector_udf = udf(lambda vector: extract_values_from_vector(vector), ArrayType(DoubleType()))

    posts = posts.withColumn("extracted_keys", extract_keys_from_vector_udf("features"))
    posts = posts.withColumn("extracted_values", extract_values_from_vector_udf("features"))

    posts = posts.withColumn("zipped_truncated",
                             slice(sort_array(arrays_zip("extracted_values", "extracted_keys"), asc=False), 1,
                                   n_keywords))

    take_second = udf(lambda rows: [row[1] for row in rows], ArrayType(IntegerType()))
    posts = posts.withColumn("top_indices", take_second("zipped_truncated"))

    return posts


def explode_group_filter(keyword_posts, vocab, n_words=50, vocab_lookup=False):
    """Takes in a dataframe with a top_indices column given indices of keywords for each post,
     along with a column CreationDate of dates for the respective posts.
    Output effectively inverts this key:value pair to produce a dataframe with one column
    keyword_index consisting of keywords which appeared in some top_indices row, and a column
    collect_list(CreationDate) consisting of dates on which a post was made with that keyword.

    n_words parameter sets cutoff for number of keywords to consider (chosen by maximal number of posts).
    vocab_lookup attaches a new column 'keyword_literal' giving the actual keyword (not the index).
    """

    exploded = keyword_posts.withColumn('keyword_index', explode('top_indices'))
    unexploded = exploded.groupby("keyword_index").agg(
        collect_list("CreationDate"))

    unexploded = unexploded.withColumn('n_posts', size(col("collect_list(CreationDate)")))
    unexploded = unexploded.sort('n_posts', ascending=False).limit(n_words)

    if vocab_lookup:
        max_index = unexploded.agg({"keyword_index": "max"}).collect()[0][0]
        small_vocab = vocab[:max_index + 1]  # vocabulary is sorted by total word count. Only lookup possible words.

        vocab_dict = {k: v for k, v in enumerate(small_vocab)}
        vocab_mapping = create_map([lit(x) for x in chain(*vocab_dict.items())])
        unexploded = unexploded.withColumn("keyword_literal", vocab_mapping.getItem(col("keyword_index")))

    unexploded = unexploded.where(length('keyword_literal') > 1)  # Remove hard to interpret single-character keywords
    return unexploded


def quiet_logs(spark):
    """Reduces quantity of spark logging to make debugging simpler"""
    logger = spark._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)
    return None


def process_all_and_write_to_redis(spark, which_tag, post_link, tags_link, redis=True):
    """Runs all of the processing steps defined above in order, for a given tag.
    Writes resulting dataframe to Redis.
    """
    top_tags = extract_top_tags(convert_tags(spark, tags_link))
    tag = top_tags[which_tag]

    cleaned_posts = convert_posts(spark, post_link)
    tag_transferred = tag_transfer(cleaned_posts)
    tag_selected = select_with_tag(tag_transferred, tag)
    #tag_selected.persist()
    output_posts, vocabulary = body_pipeline(tag_selected)

    keyworded_posts = extract_top_keywords(output_posts)['Id', 'CreationDate', 'top_indices']
    final = explode_group_filter(keyworded_posts, vocabulary, vocab_lookup=True)
    final = final['keyword_literal', 'collect_list(CreationDate)']

    print('Processing complete.')
    #tag_selected.unpersist()

    if redis:
        final.write.format("org.apache.spark.sql.redis").option(
            "table", "{}".format(tag)).option("key.column", "keyword_literal").mode("overwrite").save()

        print('TAG {} INSERTED INTO REDIS'.format(tag))
    else:
        print(tag)
        final.show()
    return None


if __name__ == "__main__":
    spark_ = SparkSession.builder.appName(
        "MainTransformation").config(
        "spark.redis.host", os.environ["REDIS_DNS"]).getOrCreate()
    quiet_logs(spark_)

    S3_bucket = os.environ["S3_BUCKET"]

    link_atoms = S3_bucket + 'two_atoms.xml'
    link_mo = S3_bucket + 'mathoverflow/Posts.xml'
    link_all = S3_bucket + 'stackoverflow/Posts.xml'

    link_mo_tags = S3_bucket + 'mathoverflow/Tags.xml'
    link_all_tags = S3_bucket + 'stackoverflow/Tags.xml'

    process_all_and_write_to_redis(spark_, 0, link_all, link_all_tags, redis=False)
