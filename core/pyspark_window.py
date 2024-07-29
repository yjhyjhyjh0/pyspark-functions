from dataclasses import dataclass
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql import functions as F


def run_partition_by(spark):
    df = spark.createDataFrame([("id1", "value1", 10), ("id1", "value2", 20), ("id2", "value3", 50)],
                               ["id", "col1", "cnt"])
    window_function = Window.partitionBy("id")
    df2 = df.withColumn("partition_sum", F.sum(F.col("cnt")).over(window_function))
    df2.show(5, False)
    # +---+------+---+-------------+
    # |id |col1  |cnt|partition_sum|
    # +---+------+---+-------------+
    # |id1|value1|10 |30           |
    # |id1|value2|20 |30           |
    # |id2|value3|50 |50           |
    # +---+------+---+-------------+


def run_partition_by_order_by(spark):
    df = spark.createDataFrame([("id1", "value1", 10), ("id1", "value2", 20), ("id2", "value3", 50)],
                               ["id", "col1", "cnt"])
    window_function = Window.partitionBy("id").orderBy("col1")
    df2 = df.withColumn("partition_order", F.rank().over(window_function))
    df2.show(5,False)
    # +---+------+---+---------------+
    # |id |col1  |cnt|partition_order|
    # +---+------+---+---------------+
    # |id1|value1|10 |1              |
    # |id1|value2|20 |2              |
    # |id2|value3|50 |1              |
    # +---+------+---+---------------+


if __name__ == '__main__':
    spark = SparkSession \
        .builder \
        .master("local") \
        .getOrCreate()
    run_partition_by(spark)
    run_partition_by_order_by(spark)
