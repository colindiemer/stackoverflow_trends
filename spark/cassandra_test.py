from pyspark.sql import SparkSession
import os


def quiet_logs(spark):
    """Reduces quantity of spark logging to make debugging simpler"""
    logger = spark._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)
    return None


if __name__ == "__main__":
    spark_ = SparkSession.builder \
        .appName('SparkCassandraApp') \
        .config('spark.cassandra.connection.host', os.environ["CASSANDRA_DNS"]) \
        .config('spark.cassandra.connection.port', '9042') \
        .config('spark.cassandra.output.consistency.level', 'ONE') \
        .master('local[2]') \
        .getOrCreate()
    quiet_logs(spark_)

    # a = [5, 4, 3, 2, 1]
    # b = [0, 2, 4, 6, 8]
    # df = spark_.createDataFrame(zip(a, b), schema=['test_a', 'test_b'])
    # df.show()
    # df.write\
    #     .format("org.apache.spark.sql.cassandra")\
    #     .mode('append')\
    #     .options(table="kv", keyspace="test")\
    #     .save()
    # print('Dataframe written to Cassandra')

    read_df = spark_.read\
        .format("org.apache.spark.sql.cassandra")\
        .options(table="emp", keyspace="dev")\
        .load().show()
    print('Dataframe read from Cassandra')
    read_df.show()
