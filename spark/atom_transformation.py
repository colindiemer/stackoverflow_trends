from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, regexp_replace, split, coalesce, array, when
from pyspark.sql.functions import lower as lower_
from pyspark.sql.types import ArrayType, StringType, IntegerType, MapType
from pyspark.ml.feature import Tokenizer, StopWordsRemover, CountVectorizer, IDF
from pyspark.ml import Pipeline
import re
import html
import psycopg2
import os


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
        cleaned = parsed.withColumn("Body_clean", clean_string(parsed['Body']))
        cleaned = cleaned.withColumn("Title_clean", clean_string(parsed['Title']))
        cleaned = cleaned.withColumn("Body_clean", blank_as_null("Body_clean"))
        cleaned = cleaned.withColumn("Title_clean", blank_as_null("Title_clean"))

        title_tokenizer = Tokenizer(inputCol="Title_clean", outputCol="Title_tokens")
        title_stop_remover = StopWordsRemover(inputCol=title_tokenizer.getOutputCol(), outputCol="Title_tokens_stopped")
        title_pipeline = Pipeline(stages=[title_tokenizer, title_stop_remover])

        title_model = title_pipeline.fit(cleaned)
        processed_title = title_model.transform(cleaned)

        body_tokenizer = Tokenizer(inputCol="Body_clean", outputCol="Body_tokens")
        body_stop_remover = StopWordsRemover(inputCol=body_tokenizer.getOutputCol(), outputCol="Body_tokens_stopped")
        body_count = CountVectorizer(inputCol=body_stop_remover.getOutputCol(), outputCol="body_features_raw")
        body_idf = IDF(inputCol=body_count.getOutputCol(), outputCol="body_features", minDocFreq=5)

        body_pipeline = Pipeline(stages=[body_tokenizer, body_stop_remover, body_count, body_idf])

        body_model = body_pipeline.fit(processed_title)
        processed_body = body_model.transform(processed_title)

        body_vocab = body_model.stages[-2].vocabulary

        return processed_body

        # body_tokenizer = Tokenizer(inputCol="Body_clean", outputCol="Body_clean_token")
        # title_tokenizer = Tokenizer(inputCol="Title_clean", outputCol="Title_clean_token")
        # cleaned = body_tokenizer.transform(cleaned)
        # cleaned = title_tokenizer.transform(cleaned)
        # title_stopwords = StopWordsRemover()
        # title_stopwords.setInputCol("Title_clean_token")
        # title_stopwords.setOutputCol("Title_final_token")
        # cleaned = title_stopwords.transform(cleaned)
        # body_stopwords = StopWordsRemover()
        # body_stopwords.setInputCol("Body_clean_token")
        # body_stopwords.setOutputCol("Body_final_token")
        # cleaned = body_stopwords.transform(cleaned)
        # return cleaned


def quiet_logs(spark):
    """Reduces quantity of spark logging to make debugging simpler"""
    logger = spark._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)
    return None


S3_bucket = os.environ["S3_BUCKET"]
POSTGRES_DNS = os.environ["POSTGRES_DNS"]
POSTGRES_PASSWORD = os.environ["POSTGRES_PASSWORD"]


def postgres_insert_atom(dataframe,
                         host=POSTGRES_DNS,
                         table='test_1',
                         password=POSTGRES_PASSWORD,
                         user='postgres'):
    """Sample insertion into PostgreSQL database. Only inserts a single atom of data. """
    atom_id = dataframe.first()[0]
    atom_body = dataframe.first()[1]
    atom_tags = dataframe.first()[2]
    atom_length = dataframe.first()[3]
    connection = psycopg2.connect(host=host, database=table, user=user,
                                  password=password)
    cursor = connection.cursor()
    try:
        cursor.execute(
            """INSERT INTO test_1 (Id, Body, Tags, Length) VALUES (%(id)s, %(body)s, %(tags)s, %(length)s);""",
            dict(id=atom_id, body=atom_body, tags=atom_tags, length=atom_length))
        connection.commit()  # Important!
        print('Inserted atom into test_1 table')
        print('Here is the current content of the test_1 Postgres table')
        cursor.execute('''SELECT * FROM test_1''')
        print(cursor.fetchall())
        cursor.close()
        connection.close()

    except psycopg2.errors.UniqueViolation:
        connection.rollback()
        print('I tried to insert this atom into test_1, but it appears to already be there.')
        print('Here is the current content of the test_1 Postgres table:')
        cursor.execute('''SELECT * FROM test_1''')
        print(cursor.fetchall())
        cursor.close()
        connection.close()

    return None


def process_atom(
        spark, link=S3_bucket + 'atom.xml', postgres_insert=True):
    """Downloads a single atom of data (i.e. a single stack exchange post).
    Then parses the XML via convert_posts, selects out a few noteworthy columns, computes a new column (length of post),
     and then attempts to insert into a Postgres table.
    """

    atom = convert_posts(spark=spark, link=link)
    atom_small = atom.select('Id', 'Body', 'Tags')
    # udf_func = create_udf(len, IntegerType)
    udf_func = udf(lambda x: len(x), returnType=IntegerType())
    atom_trans = atom_small.withColumn('Length', udf_func(atom_small.Body))
    print('Here is the dataframe schema')
    print(atom_trans)
    print("And here is the atom with .show()")
    print(atom_trans.show())
    if postgres_insert:
        postgres_insert_atom(atom_trans)

    return atom_trans


def process_two_atoms(spark, link=S3_bucket + 'two_atoms.xml'):
    atom = convert_posts(spark=spark, link=link)
    atom_small = atom.select('Id', 'Body', 'Tags')
    # udf_func = create_udf(len, IntegerType)
    udf_func = udf(lambda x: len(x), returnType=IntegerType())
    atom_trans = atom_small.withColumn('Length', udf_func(atom_small.Body))
    print('And here are two atoms:')
    print(atom_trans.show())

    return atom_trans


def process_mathoverflow(spark, link=S3_bucket + 'mathoverflow/Posts.xml', clean_text=True):
    """Load and process mathoverflow (medium sized example)"""
    mo = convert_posts(spark=spark, link=link, clean_text=clean_text)
    if not clean_text:
            mo_essential = mo['Id', 'Body', 'Title', 'Tags']
        else:
            mo_essential = mo[
                'Id', 'PostTypeId', 'ParentId', "Body_tokens_stopped", "body_features", "Title_tokens_stopped", 'Tags']
        print(mo_essential.printSchema())
        print(mo_essential.show(10))



if __name__ == "__main__":
    spark_ = SparkSession.builder.appName("MainTransformation").getOrCreate()
    quiet_logs(spark_)
    # process_atom(spark_)
    # process_two_atoms(spark_)
    process_mathoverflow(spark_)
