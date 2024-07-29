from dataclasses import dataclass
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql import functions as F





def create_fake_df(spark):
    df = spark.createDataFrame([("id1", "value1", 11, 12,13), ("id2", "value2", 201, 202,203), ],
                               ["id", "col1", "col_pivot1","col_pivot2","col_pivot3"])

    return df

# unpivot certain columns
#
def unpivot(df, base_cols =[], unpivot_cols=None, col_name="metric_name", data_col_name="data"):
    """
    unpivot dataframe cols
    :param base_cols: list of columns to use as identifier variable
    :param unpivot_cols: list of column to unpivot
    :param col_name: new metric column name to store unpivot cols
    :param data_col_name: new data column to unpviot col values
    :return:
    """
    print("melt start")
    if unpivot_cols is None:
        unpivot_cols = [column for column in df.columns if column not in base_cols]
    #
    print(unpivot_cols)
    # ['col_pivot1', 'col_pivot2', 'col_pivot3']
    # manually create array of struct, then explode
    col_and_values = F.array(*(
        F.struct(F.lit(col).alias(col_name), F.col(col).alias(data_col_name))
        for col in unpivot_cols
    ))
    print(col_and_values)
    # Column<'array(struct(col_pivot1 AS metric_name, col_pivot1 AS data), struct(col_pivot2 AS metric_name, col_pivot2 AS data), struct(col_pivot3 AS metric_name, col_pivot3 AS data))'>

    df_tmp= df.withColumn("vars", F.explode(col_and_values))
    df_tmp.cache()
    df.show(3,False)
    # +---+------+----------+----------+----------+
    # |id |col1  |col_pivot1|col_pivot2|col_pivot3|
    # +---+------+----------+----------+----------+
    # |id1|value1|11        |12        |13        |
    # |id2|value2|201       |202       |203       |
    # +---+------+----------+----------+----------+
    df_tmp.show(3,False)
    df_tmp.printSchema()
    # +---+------+----------+----------+----------+----------------+
    # |id |col1  |col_pivot1|col_pivot2|col_pivot3|vars            |
    # +---+------+----------+----------+----------+----------------+
    # |id1|value1|11        |12        |13        |{col_pivot1, 11}|
    # |id1|value1|11        |12        |13        |{col_pivot2, 12}|
    # |id1|value1|11        |12        |13        |{col_pivot3, 13}|
    # +---+------+----------+----------+----------+----------------+
    # Could use F.col("vars")[col].alias(col) to dynamically point to field in struct
    cols = base_cols + [
        F.col("vars")[col].alias(col) for col in [col_name, data_col_name]
    ]
    print(cols)
    # ['id', 'col1', Column<'vars[metric_name] AS metric_name'>, Column<'vars[data] AS data'>]
    df_tmp.select(*cols).show(3,False)
    # +---+------+-----------+----+
    # |id |col1  |metric_name|data|
    # +---+------+-----------+----+
    # |id1|value1|col_pivot1 |11  |
    # |id1|value1|col_pivot2 |12  |
    # |id1|value1|col_pivot3 |13  |
    # +---+------+-----------+----+
    df_tmp.printSchema()
    print("melt end")
    return df_tmp.select(*cols)


def main(spark):
    df = create_fake_df(spark)
    df.cache()
    df.show(3, False)
    df.printSchema()
    group_columns = ['id', 'col1']
    df_unpivot = unpivot(df, group_columns, data_col_name="data")
    print("-------")
    df_unpivot.show(3, False)
    df_unpivot.printSchema()

    # Spark 3.4 +
    # df = df.melt(['A'], ['X', 'Y', 'Z'], 'B', 'C')
    # #  OR
    # df = df.unpivot(['A'], ['X', 'Y', 'Z'], 'B', 'C')
    # df_unpivot_v2=df.unpivot(group_columns, ['col_pivot1','col_pivot2','col_pivot3'], 'metric_name', 'data')



if __name__ == '__main__':
    spark = SparkSession \
        .builder \
        .master("local") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    main(spark)
