from pyspark.sql.functions import \
    udf, col, regexp_replace, split, array, \
    when, slice, to_date, arrays_zip, sort_array, \
    explode, collect_list, count, size, create_map, \
    lit, array_contains, coalesce, size, when, \
    concat, length
from pyspark.sql.types import ArrayType, StringType, IntegerType, MapType, DoubleType, DataType
from XML_parser import parse_line


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
