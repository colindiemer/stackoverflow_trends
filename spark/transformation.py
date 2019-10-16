from pyspark.sql import SparkSession
from pyspark.sql.functions import \
    udf, col, regexp_replace, split, array, \
    when, slice, to_date, arrays_zip, sort_array, \
    explode, collect_list, count, size, create_map, \
    lit, array_contains, coalesce, size, when, \
    concat, length
from pyspark.sql.functions import lower as lower_
from pyspark.sql.types import ArrayType, StringType, IntegerType, MapType, DoubleType, DataType
from pyspark.ml.feature import Tokenizer, StopWordsRemover, CountVectorizer
from pyspark.ml import Pipeline
import nltk
from nltk.stem import WordNetLemmatizer
import re
import html
import os
import math
import wikiwords
import logging


def parse_line(line):
    regex = re.compile(' ([A-Za-z]+)="([^"]*)"')
    return {key: value for key, value in regex.findall(line)}


def convert_posts(spark, link):
    """Reads in raw XML file of stackoverflow posts from a S3 bucket link
    and processes the XML into a spark dataframe.
    """

    def read_tags_raw(tags_string):
        """converts <tag1><tag2> to ['tag1', 'tag2']"""
        return html.unescape(tags_string).strip('>').strip('<').split('><') if tags_string else []

    def read_tags():
        return udf(read_tags_raw, ArrayType(StringType()))

    def unescape():
        return udf(lambda text: html.unescape(text) if text else None)

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

    return process_text(parsed)['Id', 'ParentId', 'PostTypeId', 'CreationDate', 'Text', 'Tags']


def process_text(parsed_dataframe):
    """Given a dataframe of parsed Stackoverflow posts, performs basic text cleaning operations
     on each textual column. Also combines Title and Body into a single column Text_clean."""

    def clean_string(s):
        """Makes string lowercase, removes whitespace characters,
        removes markup tags, and removes punctuation."""
        s = lower_(s)
        s = regexp_replace(s, "\n", "")
        s = regexp_replace(s, "<[^>]*>", "")  # remove markup tags
        s = regexp_replace(s, "[^\\w\\s]", "")  # remove punctuation
        s = regexp_replace(s, "\b\\w{1,2}\b", "")  # remove small words
        return s

    def blank_as_null(x):
        """Replace Null type strings with an empty string. Useful because some SparkML transformers
        choke on Null values. """
        return when(col(x) != "", col(x)).otherwise("")

    @udf(StringType())
    def lemmatize(text):
        def lemma(word):
            lemmatizer = WordNetLemmatizer()
            return lemmatizer.lemmatize(word)

        return " ".join(list(map(lemma, text.split())))

    cleaned = parsed_dataframe.withColumn("Body", clean_string(parsed_dataframe['Body']))
    cleaned = cleaned.withColumn("Title", clean_string(parsed_dataframe['Title']))
    cleaned = cleaned.withColumn("Body", blank_as_null("Body"))
    cleaned = cleaned.withColumn("Title", blank_as_null("Title"))
    cleaned = cleaned.withColumn('Text_raw',
                                 concat(col('Body'), lit(' '), col('Title')))
    cleaned = cleaned.withColumn("CreationDate", to_date(col("CreationDate")))
    cleaned = cleaned.withColumn('Text', lemmatize(col('Text_raw')))

    return cleaned



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
    """Finds the most commonly used tags."""
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
    """Selects out those posts with a given tag"""
    return posts.where(array_contains('Tags', tag))

def total_monthly_counts_for_tag(posts_tag_selected):
    """Produces a dataframe of monthly counts of posts. In practice only to be used on posts using a single tag"""
    monthly_counts = posts_tag_selected.groupby("CreationDate").agg(count('CreationDate'))
    return monthly_counts

def generate_stopwords(tags,
                       stopwords_file="stopwords_10000.txt",
                       common_tech_words_file='common_tech_words.txt',
                       tag_min=500):
    """Generates a list of stopwords. By default takes in the file consisting of 10,000 most commmon words.
    To avoid throwing out too much, we repopulate with tech words coming from popular tags.
    Unforunately, SO has some very broad tags like "file", "web" or "function", which we then re-remove."""
    stopwords = []
    with open(stopwords_file, "r") as f:
        for line in f:
            stopwords.append(line)
    stopwords = [word.strip() for word in stopwords]

    tagwords = [tag.lower() for tag in extract_top_tags(tags, top=tag_min)]
    tagwords = [tag.split('-') for tag in tagwords]
    tagwords = [item for sublist in tagwords for item in sublist]  # flatten list
    lemmatizer = WordNetLemmatizer()
    tagwords = [lemmatizer.lemmatize(tag) for tag in tagwords]

    too_common_tag_words = []
    with open(common_tech_words_file, "r") as f:
        for line in f:
            too_common_tag_words.append(line)
    too_common_tag_words = [word.strip() for word in too_common_tag_words]

    return list(set(stopwords).difference(tagwords)) + too_common_tag_words


def pipeline(cleaned_dataframe, stopwordlist=None):
    """Pipeline for Tokenizing, removing stop words, and performing word count."""
    tokenizer = Tokenizer(inputCol="Text", outputCol="Text_tokens")
    if stopwordlist:
        stop_remover = StopWordsRemover(inputCol=tokenizer.getOutputCol(), outputCol="Text_tokens_stopped",
                                        stopWords=stopwordlist)
    else:
        stop_remover = StopWordsRemover(inputCol=tokenizer.getOutputCol(), outputCol="Text_tokens_stopped")

    count_vect = CountVectorizer(inputCol=stop_remover.getOutputCol(), outputCol="features")

    pipe_line = Pipeline(stages=[tokenizer, stop_remover, count_vect])
    model = pipe_line.fit(cleaned_dataframe)
    featurized_data = model.transform(cleaned_dataframe)

    return featurized_data, model.stages[-1].vocabulary


def array_transform(f, t=StringType()):
    """General use mapping udf"""
    if not isinstance(t, DataType):
        raise TypeError("Invalid type {}".format(type(t)))

    @udf(ArrayType(t))
    def map_(arr):
        if arr is not None:
            return [f(x) for x in arr]

    return map_


def idf_wiki(token):
    """Computed IDF score based on lookup table of frequency based on Wikipedia corpus"""
    if wikiwords.freq(token) == 0:
        return math.log(wikiwords.N)
    else:
        return math.log(wikiwords.freq(token))


def extract_top_keywords(posts, vocabulary, n_keywords=10):
    """Given word count (Count Vectorizer) output (as "features" column) -
    extracts out the vocabulary index of the 10 keywords with highest TF-IDF (for each post)."""

    def extract_keys_from_vector(vector):
        return vector.indices.tolist()

    def extract_values_from_vector(vector):
        return vector.values.tolist()

    extract_keys_from_vector_udf = udf(lambda vector: extract_keys_from_vector(vector), ArrayType(IntegerType()))
    extract_values_from_vector_udf = udf(lambda vector: extract_values_from_vector(vector), ArrayType(DoubleType()))

    idf_udf = array_transform(idf_wiki)
    vocab_dict = {k: v for k, v in enumerate(vocabulary)}

    def ix_to_word(ix):
        return vocab_dict[ix]

    vocab_udf = array_transform(ix_to_word)

    posts = posts.withColumn("word_ix", extract_keys_from_vector_udf("features"))
    posts = posts.withColumn("word_count", extract_values_from_vector_udf("features"))
    posts = posts.withColumn('words', vocab_udf(col('word_ix')))
    posts = posts.withColumn("idf", idf_udf(col("words")))
    posts = posts.withColumn("zipped_truncated",
                             slice(sort_array(arrays_zip("idf", "words"), asc=False), 1,
                                   n_keywords))

    take_second = udf(lambda rows: [row[1] for row in rows], ArrayType(StringType()))
    posts = posts.withColumn("top_keywords", take_second("zipped_truncated"))

    return posts['CreationDate', 'top_keywords', 'Tags', 'ParentId']


def explode_group_filter(keyword_posts, n_keywords=50):
    """Creates a list of the top 50 (up to some filtering) keywords by number of uses.
    Output is a dataframe with those keywords as one column, and the time series of associate posts as another."""
    exploded = keyword_posts.withColumn('keyword', explode('top_keywords'))
    counted = exploded.groupby("keyword").agg(count('CreationDate'))
    top_keywords = counted \
        .sort('count(CreationDate)', ascending=False) \
        .limit(n_keywords) \
        .select("keyword").rdd.flatMap(lambda x: x).collect()
    exploded_filtered = exploded.filter(exploded.keyword.isin(top_keywords))
    unexploded = exploded_filtered.groupby("keyword").agg(collect_list("CreationDate"))
    unexploded = unexploded.where(length('keyword') > 2)  # Remove confusing one or two character "keywords"

    return unexploded


def process_all_and_write(spark, which_tag, post_link, tags_link, log, redis=True, cassandra=True, ):
    """Runs all of the processing steps defined above in order, for a given tag.
    Writes resulting dataframe to Redis.
    """

    processed_tags = convert_tags(spark, tags_link)
    stopwords = generate_stopwords(processed_tags)

    top_tags = extract_top_tags(processed_tags)
    tag = top_tags[which_tag]

    cleaned_posts = convert_posts(spark, post_link)

    tag_transferred = tag_transfer(cleaned_posts)
    tag_selected = select_with_tag(tag_transferred, tag)
    tag_selected.cache()
    log.info('Posts with TAG: {} selected and text has been cleaned'.format(tag))

    output_posts, vocabulary = pipeline(tag_selected, stopwordlist=stopwords)
    log.info('Posts with TAG: {} selected have been processed through TF-IDF pipeline'.format(tag))

    keyworded_posts = extract_top_keywords(output_posts, vocabulary)

    final = explode_group_filter(keyworded_posts)
    final = final \
        .withColumn("tag", lit(tag)) \
        .withColumnRenamed('collect_list(CreationDate)', 'dates') \
        .where(final.keyword != '{}'.format(tag))

    final_keywords = final.select("keyword").rdd.flatMap(lambda x: x).collect()
    logger.info("Keywords extracted for TAG {0} are: {1}".format(tag, ', '.join(final_keywords)))

    tag_selected.unpersist()

    log.info('Top KEYWORDS for TAG: {} have been extracted and associated time series collected'.format(tag))

    if redis:
        final.write.format("org.apache.spark.sql.redis").option(
            "table", "{}".format(tag)).option("key.column", "keyword").mode("overwrite").save()

        log.info('TAG: {} INSERTED INTO REDIS'.format(tag))

    if cassandra:
        final.write \
            .format("org.apache.spark.sql.cassandra") \
            .mode('append') \
            .options(table=os.environ['CASSANDRA_TABLE'], keyspace=os.environ['CASSANDRA_KEYSPACE']) \
            .save()
        log.info('TAG: {} INSERTED INTO CASSANDRA'.format(tag))

    if (not cassandra) and (not redis):
        log.info('No Database insertion for TAG {} performed'.format(tag))

    log.info("ALL DONE WITH TAG {}".format(tag))
    return None


def quiet_spark_logs(spark):
    """Reduces quantity of spark logging to make debugging simpler"""
    spark_logger = spark._jvm.org.apache.log4j
    spark_logger.LogManager.getLogger("org").setLevel(spark_logger.Level.ERROR)
    spark_logger.LogManager.getLogger("akka").setLevel(spark_logger.Level.ERROR)
    return None


if __name__ == "__main__":
    spark_ = SparkSession.builder.appName("MainTransformation") \
        .config("spark.sql.shuffle.partitions", "500") \
        .config("spark.redis.host", os.environ["REDIS_DNS"]) \
        .config('spark.redis.db', os.environ["REDIS_DB"]) \
        .config('spark.cassandra.connection.host', os.environ["CASSANDRA_DNS"]) \
        .config('spark.cassandra.connection.port', os.environ["CASSANDRA_PORT"]) \
        .config('spark.cassandra.output.consistency.level', 'ONE') \
        .getOrCreate()

    quiet_spark_logs(spark_)

    logger = logging.getLogger(__name__)
    logger.setLevel("INFO")
    formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(message)s')
    file_handler = logging.FileHandler('spark_job.log')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    nltk.download("wordnet")

    S3_bucket = os.environ["S3_BUCKET"]

    link_all = S3_bucket + 'stackoverflow/Posts.xml'

    link_all_tags = S3_bucket + 'stackoverflow/Tags.xml'
    for i in range(40):
        process_all_and_write(spark_, i, link_all, link_all_tags, logger, redis=True, cassandra=False)
