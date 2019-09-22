from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import ArrayType, StringType, IntegerType, MapType
import re
import html
import psycopg2
import os


def read_tags_raw(tags_string):  # converts <tag1><tag2> to ['tag1', 'tag2']
    return html.unescape(tags_string).strip('>').strip('<').split('><') if tags_string else []


pattern = re.compile(' ([A-Za-z]+)="([^"]*)"')


# parse_line = lambda line: {key: value for key, value in pattern.findall(line)}
def parse_line(line):
    return {key: value for key, value in pattern.findall(line)}


unescape = udf(lambda escaped: html.unescape(escaped) if escaped else None)

read_tags = udf(read_tags_raw, ArrayType(StringType()))


def convert_posts(spark, link):
    """Reads in raw XML file of posts from a link and processes the XML into a spark dataframe.
    """
    return spark.read.text(link).where(col('value').like('%<row Id%')) \
        .select(udf(parse_line, MapType(StringType(), StringType()))('value').alias('value')) \
        .select(
        col('value.Id').cast('integer'),
        col('value.PostTypeId').cast('integer'),
        col('value.ParentId').cast('integer'),
        col('value.AcceptedAnswerId').cast('integer'),
        col('value.CreationDate').cast('timestamp'),
        col('value.Score').cast('integer'),
        col('value.ViewCount').cast('integer'),
        unescape('value.Body').alias('Body'),
        col('value.OwnerUserId').cast('integer'),
        col('value.LastEditorUserId').cast('integer'),
        col('value.LastEditorDisplayName'),
        col('value.LastEditDate').cast('timestamp'),
        col('value.LastActivityDate').cast('timestamp'),
        col('value.CommunityOwnedDate').cast('timestamp'),
        col('value.ClosedDate').cast('timestamp'),
        unescape('value.Title').alias('Title'),
        read_tags('value.Tags').alias('Tags'),
        col('value.AnswerCount').cast('integer'),
        col('value.CommentCount').cast('integer'),
        col('value.FavoriteCount').cast('integer')
    )


def create_udf(function, returntype):
    """Generic template for producing udf's for spark"""
    return udf(lambda x: function(x), returnType=returntype())


def quiet_logs(spark):
    """Reduces quantity of spark logging to make debugging simpler"""
    logger = spark._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)
    return None


def process_atom(spark, link=S3_bucket+'atom.xml', , host=POSTGRES_DNS, table='test_1', password=POSTGRES_PASSWORD, user='postgres'):
    """Downloads a single atom of data (i.e. a single stack exchange post).
    Then parses the XML via convert_posts, selects out a few noteworthy columns, computes a new column (length of post),
     and then attempts to insert into a Postgres table.
    """

    atom = convert_posts(spark=spark, link=link)
    atom_small = atom.select('Id', 'Body', 'Tags')
    udf_func = create_udf(len, IntegerType)
    atom_trans = atom_small.withColumn('Length', udf_func(atom_small.Body))
    print('Here is the dataframe schema')
    print(atom_trans)
    print("And here is the atom with .show()")
    print(atom_trans.show())
    atom_Id = atom_trans.first()[0]
    atom_Body = atom_trans.first()[1]
    atom_Tags = atom_trans.first()[2]
    atom_Length = atom_trans.first()[3]
    connection = psycopg2.connect(host=host, database=table, user=user,
                                  password=password)
    cursor = connection.cursor()
    try:
        cursor.execute(
            """INSERT INTO test_1 (Id, Body, Tags, Length) VALUES (%(id)s, %(body)s, %(tags)s, %(length)s);""",
            dict(id=atom_Id, body=atom_Body, tags=atom_Tags, length=atom_Length))
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

    return atom_trans


def process_two_atoms(spark, link= S3_bucket+'two_atoms.xml'):
    atom = convert_posts(spark=spark, link=link)
    atom_small = atom.select('Id', 'Body', 'Tags')
    udf_func = create_udf(len, IntegerType)
    atom_trans = atom_small.withColumn('Length', udf_func(atom_small.Body))
    print('And here are two atoms:')
    print(atom_trans.show())

    return atom_trans


if __name__ == "__main__":

    S3_bucket = os.environ("S3_BUCKET")
    POSTGRES_PASSWORD = os.environ("POSTGRES_PASSWORD")
    POSTGRES_DNS = os.environ("POSTGRES_DNS")

    spark_ = SparkSession.builder.appName("MainTransformation").getOrCreate()
    quiet_logs(spark_)
    process_atom(spark_)
    process_two_atoms(spark_)

