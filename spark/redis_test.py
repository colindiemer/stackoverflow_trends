from pyspark.sql import SparkSession
import os


def quiet_logs(spark):
    """Reduces quantity of spark logging to make debugging simpler"""
    logger = spark._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)
    return None


if __name__ == "__main__":
    spark = SparkSession.builder.appName(
        "RedisTestWrite").config("spark.redis.host", os.environ["POSTGRES_DNS"]).getOrCreate()

    a = [5, 4, 3, 2, 1]
    b = [0, 2, 4, 6, 8]
    df = spark.createDataFrame(zip(a, b), schema=['test_a', 'test_b'])
    quiet_logs(spark)

    df.show()

    df.write.format("org.apache.spark.sql.redis").option(
       "table", "test_table_1").option("key.column", "test_a").save()
