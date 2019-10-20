from pyspark.sql.functions import \
    udf, col, regexp_replace, split, array, \
    when, slice, to_date, arrays_zip, sort_array, \
    explode, collect_list, count, size, create_map, \
    lit, array_contains, coalesce, size, when, \
    concat, length
from pyspark.sql.functions import lower as lower_
from pyspark.sql.types import ArrayType, StringType, IntegerType, MapType, DoubleType, DataType
from nltk.stem import WordNetLemmatizer
import re
import html


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


