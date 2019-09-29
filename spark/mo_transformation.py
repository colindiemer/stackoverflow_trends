from pyspark.sql import SparkSession
from pyspark.sql.functions import \
    udf, col, regexp_replace, split, coalesce, array, \
    when, slice, lit, to_date, arrays_zip, sort_array, \
    explode, collect_list, count
from pyspark.sql.functions import lower as lower_
from pyspark.sql.types import ArrayType, StringType, IntegerType, MapType, DoubleType
from pyspark.ml.feature import Tokenizer, StopWordsRemover, CountVectorizer, IDF
from pyspark.ml import Pipeline
import re
import html
import os
import redis


def read_tags_raw(tags_string):
    """converts <tag1><tag2> to ['tag1', 'tag2']"""
    return html.unescape(tags_string).strip('>').strip('<').split('><') if tags_string else []


def read_tags():
    return udf(read_tags_raw, ArrayType(StringType()))


def parse_line(line):
    regex = re.compile(' ([A-Za-z]+)="([^"]*)"')
    return {key: value for key, value in regex.findall(line)}


def clean_string(s):
    """Performs the following in PySpark: makes string lowercase, removes whitespace characters,
    removes markup tags, and removes punctuation."""
    s = lower_(s)
    s = regexp_replace(s, "\n", "")
    s = regexp_replace(s, "<[^>]*>", "")  # remove markup tags
    s = regexp_replace(s, "[^\w\s]", "")
    return s


def unescape():
    return udf(lambda text: html.unescape(text) if text else None)


def blank_as_null(x):
    """Replace Null type strings with an empty string. Useful because some SparkML transformers
    choke on Null values. """
    return when(col(x) != "", col(x)).otherwise("")


def convert_posts(spark, link, clean_text=True):
    """Reads in raw XML file of posts from a link and processes the XML into a spark dataframe.
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


def process_text(parsed_dataframe):
    """Given a dataframe of parsed Stackoverflow posts, performs basic NLP cleaning operations
     (those in clean_string as well as removing stop words and splitting into tokens.
     Tokenized posts and titles are then fed into TF-IDF."""

    cleaned = parsed_dataframe.withColumn("Body_clean", clean_string(parsed_dataframe['Body']))
    cleaned = cleaned.withColumn("Title_clean", clean_string(parsed_dataframe['Title']))
    cleaned = cleaned.withColumn("Body_clean", blank_as_null("Body_clean"))
    cleaned = cleaned.withColumn("Title_clean", blank_as_null("Title_clean"))
    cleaned = cleaned.withColumn("creation_date_only", to_date(col("CreationDate")))

    return cleaned


def body_pipeline(cleaned_dataframe):
    body_tokenizer = Tokenizer(inputCol="Body_clean", outputCol="Body_tokens")
    body_stop_remover = StopWordsRemover(inputCol=body_tokenizer.getOutputCol(), outputCol="Body_tokens_stopped")
    body_count = CountVectorizer(inputCol=body_stop_remover.getOutputCol(), outputCol="body_counts_raw")
    body_idf = IDF(inputCol=body_count.getOutputCol(), outputCol="features")

    pipeline = Pipeline(stages=[body_tokenizer, body_stop_remover, body_count, body_idf])

    body_model = pipeline.fit(cleaned_dataframe)
    featurized_data = body_model.transform(cleaned_dataframe)

    body_vocab = body_model.stages[-2].vocabulary

    return featurized_data, body_vocab


def extract_top_keywords(posts, n_keywords=10):
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


def quiet_logs(spark):
    """Reduces quantity of spark logging to make debugging simpler"""
    logger = spark._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)
    return None


def write_to_redis(host, dataframe, vocab, cutoff=10000):
    """Still in debugging stage; should not collect first, much less double loop! SAD!"""
    r = redis.StrictRedis(host=host, port=6379, db=0)
    id_tokens = [(row.Id, row.Body_tokens_stopped) for row in dataframe.limit(cutoff).collect()]
    for word in vocab[:100]:
        for id_, tokens in id_tokens:
            if word in tokens:
                r.rpush(word, id_)
    # print('Inserted into redis!')
    # print('Keys:')
    # print(vocab[:100])
    return None


if __name__ == "__main__":
    spark_ = SparkSession.builder.appName("MainTransformation").getOrCreate()
    quiet_logs(spark_)

    S3_bucket = os.environ["S3_BUCKET"]

    link_atoms = S3_bucket + 'two_atoms.xml'
    link_mo = S3_bucket + 'mathoverflow/Posts.xml'
    link_all = S3_bucket + 'Posts.xml'

    cols = ['Id',
            'PostTypeId',
            'Body',
            "features",
            "zipped_truncated",
            "keyword_index"]

    cleaned_posts = convert_posts(spark_, link_mo)
    output_posts, vocab_ = body_pipeline(cleaned_posts)
    keyworded_posts = extract_top_keywords(output_posts)

    exploded = keyworded_posts.withColumn('keyword_index', explode('top_indices'))

    # print(exploded.printSchema())
    # print(exploded[cols].show(12))

    unexploded = exploded.groupby("keyword_index").agg(
        collect_list("Id"), count("Id"), collect_list("PostTypeId"), count("PostTypeId"))

    print(unexploded.show(10))

    #.agg(fn.mean("dep_time").alias("avg_dep"), fn.mean("arr_time")

    # write_to_redis(os.environ["POSTGRES_DNS"], posts, vocab_)
