from pyspark.sql import SparkSession
from pyspark.sql.types import StringType


def get_local_spark():
    spark = SparkSession \
        .builder \
        .master("local") \
        .getOrCreate()
    return spark
