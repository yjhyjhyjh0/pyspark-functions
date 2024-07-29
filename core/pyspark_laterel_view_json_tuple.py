from pyspark.sql import functions as F
from pyspark.sql import SparkSession


# Goal
# Step1-Parse string type column with json schema
# Step2-Performance desire json operation on json type column


def create_fake_df(spark):
    # df = spark.createDataFrame([("id1", "{\"object_field1\":\"value1\",\"object_field2_array\":[{\"array_object_field1\":\"array_object_value1\"}]}", 10),
    #                             ("id2", "{\"object_field1\":\"value2\",\"object_field2_array\":[{\"array_object_field1\":\"array_object_value1\"}]}", 20)
    #                             ], ["id", "json_string_col", "col3"])
    df = spark.createDataFrame([("id1", "{\"object_field1\":\"value1\",\"object_field2\":\"value2\"}", 10),
                                ("id2", "{\"object_field1\":\"value2\",\"object_field2\":\"value2\"}", 20)
                                ], ["id", "json_string_col", "col3"])
    return df



def run_json_schema(spark):
    df = create_fake_df(spark)
    df.show(3, False)
    df.printSchema()
    # +---+--------------------------------------------------+----+
    # |id |json_string_col                                   |col3|
    # +---+--------------------------------------------------+----+
    # |id1|{"object_field1":"value1","object_field2":"value2"|10  |
    # |id2|{"object_field1":"value2","object_field2":"value2"|20  |
    # +---+--------------------------------------------------+----+
    df.createOrReplaceTempView("df")
    spark.sql("""
    select 
    id,
    col3,
    object_field1,
    object_field2
    from
    df a
    lateral view json_tuple(a.json_string_col, 'object_field1', 'object_field2') b
    as object_field1, object_field2
    """).show(30,False)
    # +---+----+-------------+-------------+
    # |id |col3|object_field1|object_field2|
    # +---+----+-------------+-------------+
    # |id1|10  |value1       |value2       |
    # |id2|20  |value2       |value2       |
    # +---+----+-------------+-------------+


if __name__ == '__main__':
    spark = SparkSession \
        .builder \
        .master("local") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    run_json_schema(spark)
