from pyspark.sql import SparkSession
from pyspark.sql.functions import \
    udf, col, regexp_replace, split, coalesce, array, when, slice, lit, to_date
from pyspark.sql.functions import lower as lower_
from pyspark.sql.types import ArrayType, StringType, IntegerType, MapType, DoubleType
from pyspark.ml.feature import Tokenizer, StopWordsRemover, CountVectorizer, IDF
from pyspark.ml.clustering import LDA
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


def vector_to_array(col):
    """Converts vector type to python list.
    Seemingly necessary to work with SparseVector outputs of IDF Frustratingly, there seems to be no way to avoid a udf here."""
    return udf(lambda x: x.toArray().tolist(), ArrayType(DoubleType()))(col)
    # return udf(lambda x: x.toArray(), ArrayType(DoubleType()))(col)


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

    # lda = LDA(k=10, maxIter=10)
    # lda_model = lda.fit(featurized_data)
    # top_doc_table = lda_model.transform(featurized_data)
    #
    # topics = lda_model.topicsMatrix().toArray()
    # featurized_data = featurized_data.withColumn('body_idf', vector_to_array(body_idf.getOutputCol()))

    return featurized_data, body_vocab


def quiet_logs(spark):
    """Reduces quantity of spark logging to make debugging simpler"""
    logger = spark._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)
    return None


def jdbc_write_postgresql(dataframe, db_url, table, mode, properties):
    dataframe.write.jdbc(url=db_url, table=table, mode=mode, properties=properties)
    return None


def write_to_redis(host, dataframe, vocab, cutoff=10000):
    """Still in debugging stage; should not collect first, much less double loop! SAD!"""
    r = redis.StrictRedis(host=host, port=6379, db=0)
    id_tokens = [(row.Id, row.Body_tokens_stopped) for row in dataframe.limit(cutoff).collect()]
    for word in vocab[:100]:
        for id_, tokens in id_tokens:
            if word in tokens:
                r.rpush(word, id_)
    print('Inserted into redis!')
    print('Keys:')
    print(vocab[:100])
    return None


if __name__ == "__main__":
    spark_ = SparkSession.builder.appName("MainTransformation").getOrCreate()
    quiet_logs(spark_)

    S3_bucket = os.environ["S3_BUCKET"]
    link_atoms = S3_bucket + 'two_atoms.xml'
    link_mo = S3_bucket + 'mathoverflow/Posts.xml'
    link_all = S3_bucket + 'Posts.xml'

    cols = ['Id', 'PostTypeId', 'Body', 'Body_tokens_stopped', "features"]

    cleaned_posts = convert_posts(spark_, link_atoms)
    output_posts, vocab = body_pipeline(cleaned_posts)

    print(output_posts[cols].show(10))
    print(vocab)

    # write_to_redis(os.environ["POSTGRES_DNS"], posts, vocab_)
    # postgres_url = os.environ["POSTGRES_DNS"] + ':5432/'
    # postgres_password = os.environ["POSTGRES_PASSWORD"]
    # properties_ = {'user': 'postgres', 'password': postgres_password, 'driver': 'org.postgresql.Driver'}
    # jdbc_write_postgresql(df, 'jdbc:%s' % postgres_url, 'jdbc_test', 'overwrite', properties_)
