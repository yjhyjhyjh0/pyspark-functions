from dataclasses import dataclass
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession

@dataclass
class simple_compute_input:
    df1: DataFrame
    df2: DataFrame


def read_input(spark):
    df1 = spark.createDataFrame([(1, "value1")], ["id", "col1"])
    df2 = spark.createDataFrame([(1, "value2")], ["id", "col2"])
    input = simple_compute_input(df1, df2)
    return input


def calculation(input):
    df_output = input.df1.join(input.df2, ["id"], "inner")
    return df_output


def write_output(df, write_flag):
    if write_flag:
        df.show(5, False)


def run(input, write_flag=True):
    df = calculation(input)
    write_output(df, write_flag)
    return df


if __name__ == '__main__':
    spark = SparkSession \
        .builder \
        .master("local") \
        .getOrCreate()
    input = read_input(spark)
    run(input)
