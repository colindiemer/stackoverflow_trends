from pyspark.sql import SparkSession
from pyspark.sql.functions import \
    udf, col, regexp_replace, split, array, \
    when, slice, to_date, arrays_zip, sort_array, \
    explode, collect_list, count, size, create_map, \
    lit, array_contains, coalesce, size, when
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
    top = converted_tags.orderBy("Count", ascending=False).limit(top)
    return top.select("TagName").rdd.flatMap(lambda x: x).collect()


# def tag_transfer(posts):
#     no_nulls = posts.withColumn('Tags', coalesce('Tags', array()))
#     filled = no_nulls.withColumn(
#         "Tags_filled", when(
#             (size(col("Tags")) == 0),
#             #array([lit('X')])
#         ).otherwise(col('Tags'))
#     )
#     return filled


def process_text(parsed_dataframe):
    """Given a dataframe of parsed Stackoverflow posts, performs basic text cleaning operations
     on each textual column"""

    def clean_string(s):
        """Performs the following in PySpark: makes string lowercase, removes whitespace characters,
        removes markup tags, and removes punctuation."""
        s = lower_(s)
        s = regexp_replace(s, "\n", "")
        s = regexp_replace(s, "<[^>]*>", "")  # remove markup tags
        s = regexp_replace(s, "[^\\w\\s]", "")  # remove punctuation
        s = regexp_replace(s, "\b\\w{1,2}\b", "")
        return s

    cleaned = parsed_dataframe.withColumn("Body_clean", clean_string(parsed_dataframe['Body']))
    cleaned = cleaned.withColumn("Title_clean", clean_string(parsed_dataframe['Title']))
    cleaned = cleaned.withColumn("Body_clean", blank_as_null("Body_clean"))
    cleaned = cleaned.withColumn("Title_clean", blank_as_null("Title_clean"))
    cleaned = cleaned.withColumn("creation_date_only", to_date(col("CreationDate")))

    return cleaned


def body_pipeline(cleaned_dataframe):
    """NLP pipeline. Tokenizes, removes stopwords, and computes TF-IDF
    Returns transformed data and the vocabulary of words."""

    body_tokenizer = Tokenizer(inputCol="Body_clean", outputCol="Body_tokens")
    body_stop_remover = StopWordsRemover(inputCol=body_tokenizer.getOutputCol(), outputCol="Body_tokens_stopped")
    body_count = CountVectorizer(inputCol=body_stop_remover.getOutputCol(), outputCol="body_counts_raw")
    body_idf = IDF(inputCol=body_count.getOutputCol(), outputCol="features")

    pipeline = Pipeline(stages=[body_tokenizer, body_stop_remover, body_count, body_idf])

    body_model = pipeline.fit(cleaned_dataframe)
    featurized_data = body_model.transform(cleaned_dataframe)

    return featurized_data, body_model.stages[-2].vocabulary


def extract_top_keywords(posts, n_keywords=10):
    """Given TF-IDF output (as "features" column) extracts out the index location of the
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


def explode_group_filter(keyword_posts, vocab, threshold=1000, vocab_lookup=False):
    exploded = keyword_posts.withColumn('keyword_index', explode('top_indices'))
    unexploded = exploded.groupby("keyword_index").agg(
        collect_list("creation_date_only"))

    unexploded = unexploded.where(size(col("collect_list(creation_date_only)")) > threshold)

    if vocab_lookup:
        vocab_dict = {k: v for k, v in enumerate(vocab)}
        vocab_mapping = create_map([lit(x) for x in chain(*vocab_dict.items())])
        unexploded = unexploded.withColumn("keyword_literal", vocab_mapping.getItem(col("keyword_index")))

    return unexploded


def quiet_logs(spark):
    """Reduces quantity of spark logging to make debugging simpler"""
    logger = spark._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)
    return None


if __name__ == "__main__":
    spark_ = SparkSession.builder.appName(
        "MainTransformation").config(
        "spark.redis.host", os.environ["POSTGRES_DNS"]).getOrCreate()
    quiet_logs(spark_)

    S3_bucket = os.environ["S3_BUCKET"]

    link_atoms = S3_bucket + 'two_atoms.xml'
    link_mo = S3_bucket + 'mathoverflow/Posts.xml'
    link_all = S3_bucket + 'stackoverflow/Posts.xml'

    link_mo_tags = S3_bucket + 'mathoverflow/Tags.xml'
    link_all_tags = S3_bucket + 'stackoverflow/Tags.xml'

    top_tags = extract_top_tags(convert_tags(spark_, link_mo_tags))
    top_tag = top_tags[0]

    print(type(top_tag))
    print(top_tag)

    cleaned_posts = convert_posts(spark_, link_all)
    print(cleaned_posts['Id', 'ParentId', 'Tags'].show())

    #print(tag_transfer(cleaned_posts)['PostTypeId', 'ParentId', 'Tags_filled'].show())

    # tag_filtered_posts.show()

    # output_posts, vocab_ = body_pipeline(cleaned_posts)
    # keyworded_posts = extract_top_keywords(output_posts)
    # final = explode_group_filter(keyworded_posts, vocab_)
    #
    # final.show()
    # print(final.count())
    # #print(vocab_[:50])
    #
    # final.write.format("org.apache.spark.sql.redis").option(
    #    "table", "mo_test_1").option("key.column", "keyword_index").mode("overwrite").save()

    # test_read = spark_.read.format(
    #     "org.apache.spark.sql.redis").option(
    #     "keys.pattern", "mo_test_1:*").option(
    #     "infer.schema", True).option(
    #     "key.column", "keyword_index").load()
    #
    # print(test_read.printSchema())
    # print(test_read.show())
