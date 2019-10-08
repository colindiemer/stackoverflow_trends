from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, regexp_replace, split, coalesce, array, when
from pyspark.sql.functions import lower as lower_
from pyspark.sql.types import ArrayType, StringType, IntegerType, MapType
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


def convert_posts(spark, link):
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

    return parsed


def quiet_logs(spark):
    """Reduces quantity of spark logging to make debugging simpler"""
    logger = spark._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)
    return None


def postgres_insert_atom(dataframe, host, table, password, user):
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
        spark, link, host, table, password, user, postgres_insert=True):
    """Downloads a single atom of data (i.e. a single stack exchange post).
    Then parses the XML via convert_posts, selects out a few noteworthy columns, computes a new column (length of post),
     and then attempts to insert into a Postgres table.
    """

    atom = convert_posts(spark=spark, link=link)
    atom_small = atom.select('Id', 'Body', 'Tags')
    udf_func = udf(lambda x: len(x), returnType=IntegerType())
    atom_trans = atom_small.withColumn('Length', udf_func(atom_small.Body))
    print('Here is the dataframe schema')
    print(atom_trans)
    print("And here is the atom with .show()")
    print(atom_trans.show())
    if postgres_insert:
        postgres_insert_atom(atom_trans, host, table, password, user)

    return atom_trans


def process_two_atoms(spark, link):
    atom = convert_posts(spark=spark, link=link)
    atom_small = atom.select('Id', 'Body', 'Tags')
    udf_func = udf(lambda x: len(x), returnType=IntegerType())
    atom_trans = atom_small.withColumn('Length', udf_func(atom_small.Body))
    print('And here are two atoms:')
    print(atom_trans.show())

    return atom_trans


if __name__ == "__main__":
    spark_ = SparkSession.builder.appName("MainTransformation").getOrCreate()
    quiet_logs(spark_)

    S3_bucket = os.environ["S3_BUCKET"]
    host_ = os.environ["POSTGRES_DNS"]
    password_ = os.environ["POSTGRES_PASSWORD"]
    table_ = 'test_1'
    user_ = 'postgres'
    link_1 = S3_bucket + 'atom.xml'
    link_2 = S3_bucket + 'two_atoms.xml'

    process_atom(spark_, link_1, host_, table_, password_, user_)
    process_two_atoms(spark_, link_2)
