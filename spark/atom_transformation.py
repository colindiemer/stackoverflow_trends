from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import re
import html
import psycopg2


def read_tags_raw(tags_string):  # converts <tag1><tag2> to ['tag1', 'tag2']
    return html.unescape(tags_string).strip('>').strip('<').split('><') if tags_string else []


pattern = re.compile(' ([A-Za-z]+)="([^"]*)"')
parse_line = lambda line: {key: value for key, value in pattern.findall(line)}
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


def create_udf(function, returnType):
    """Generic template for producing udf's for spark"""
    return udf(lambda x: function(x), returnType=returnType())


def quiet_logs(spark):
    """Reduces quantity of spark logging to make debugging simpler"""
    logger = spark._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)
    return None


def process_atom(link='s3a://stackoverflow.insight/atom.xml'):
    """Downloads a single atom of data (i.e. a single stack exchange post).
    Then parses the XML via convert_posts, selects out a few noteworthy columns, computes a new column (length of post),
     and then attempts to insert into a Postgres table.
     Makes various print statements to help with debugging.
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
    connection = psycopg2.connect(host='ec2-3-231-23-29.compute-1.amazonaws.com', database='test_1', user='postgres',
                                  password='plastik')
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


if __name__ == "__main__":
    spark = SparkSession.builder.appName("MainTransformation").getOrCreate()
    quiet_logs(spark)
    process_atom()

    # DEPRECATED CODE
    # atom = convert_posts(spark=spark, link='s3a://stackoverflow.insight/atom.xml')
    # atom_small = atom.select('Id', 'Body', 'Tags')
    # udf_func = create_udf(len, IntegerType)
    # atom_trans = atom_small.withColumn('Length', udf_func(atom_small.Body))

    # print('Here is the dataframe schema')
    # print(atom_trans)
    # print("And here is the atom with .show()")
    # print(atom_trans.show())
    # for item in atom_trans.first():
    #     print(item)

    # atom_Id = atom_trans.first()[0]
    # atom_Body = atom_trans.first()[1]
    # atom_Tags = atom_trans.first()[2]
    # atom_Length = atom_trans.first()[3]

    # connection = psycopg2.connect(host='ec2-3-231-23-29.compute-1.amazonaws.com', database='test_1', user='postgres',
    #                               password='plastik')
    # cursor = connection.cursor()

    # Sample postgres insertion
    # cursor.execute("""INSERT INTO test_1 (Id, Body, Tags, Length) VALUES (%(id)s, %(body)s, %(tags)s, %(length)s);""",
    #               {'id': atom_Id, 'body': atom_Body, 'tags': atom_Tags, 'length': atom_Length})
    # connection.commit()  # Important!
    # cursor.execute('''SELECT * FROM test_1''')
    # print(cursor.fetchall())
    # cursor.close()
    # connection.close()

#     spark = SparkSession.builder.appName("MainTransformation").getOrCreate()
#     converted = convert_posts(spark, 's3a://stackoverflow.insight/mathoverflow/Posts.xml')
#     atom = converted.first()
#     print("The first row of your dataframe is:")
#     print(atom)
#     print(type(atom))
