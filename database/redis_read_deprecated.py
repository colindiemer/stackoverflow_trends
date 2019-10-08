import redis
import os
from datetime import datetime


def read_one_from_redis(table, keyword_index):
    redis_hash = table + ":" + keyword_index
    read = r.hgetall(redis_hash)
    dates_extract = list(read.values())[0].decode("utf-8")  # extract bytecode
    dates_only = dates_extract[13:-1]  # Removes WrappedArray(...) bytecode
    dates_split = [s.strip() for s in dates_only.split(',')]
    dates_formatted = [datetime.strptime(date, '%Y-%m-%d').date() for date in dates_split]
    return dates_formatted


if __name__ == "__main__":
    r = redis.Redis(host=os.environ["POSTGRES_DNS"], port=6379, db=0)

    test = read_one_from_redis('mo_test_1', '349')

    print(test[:5])

# from pyspark.sql import SparkSession
# import os
#
#
# def quiet_logs(spark):
#     """Reduces quantity of spark logging to make debugging simpler"""
#     logger = spark._jvm.org.apache.log4j
#     logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
#     logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)
#     return None
#
# def read_redis(spark, table, column="keyword_index"):
#
#     table_str = table+":*"
#     df = spark.read.format(
#         "org.apache.spark.sql.redis").option(
#         "keys.pattern", table_str).option(
#         "infer.schema", True).option(
#         "key.column", column).load()
#
#     return df
#
#
# if __name__ == "__main__":
#     spark = SparkSession.builder.appName(
#         "RedisTestWrite").config("spark.redis.host", os.environ["POSTGRES_DNS"]).getOrCreate()
#
#     read = read_redis(spark, "mo_test_1:349")
