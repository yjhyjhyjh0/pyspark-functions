from pyspark.sql import functions as F
from pyspark.sql import SparkSession


# Goal
# Step1-Parse string type column with json schema
# Step2-Performance desire json operation on json type column


def create_fake_df(spark):
    df = spark.createDataFrame([("id1", "{\"object_field1\":\"value1\",\"object_field2_array\":[{\"array_object_field1\":\"array_object_value1\"}]}", 10),
                                ("id2", "{\"object_field1\":\"value2\",\"object_field2_array\":[{\"array_object_field1\":\"array_object_value1\"}]}", 20)
                                ], ["id", "json_string_col", "col3"])
    return df


def read_json_schema(spark, df):
    json_schema = spark.read.json(df.rdd.map(lambda row: row.json_string_col)).schema
    return json_schema


def from_json_explode(spark, df, json_schema):
    df_explode = df.withColumn("json_col", F.from_json(F.col("json_string_col"), json_schema)) \
        .withColumn("json_explode", F.explode(F.col("json_col.object_field2_array")))
    return df_explode


def run_json_schema(spark):
    df = create_fake_df(spark)
    df.show(3, False)
    df.printSchema()
    # +---+------------------------------------------------------------------------------------------------+----+
    # | id | json_string_col | col3 |
    # +---+------------------------------------------------------------------------------------------------+----+
    # | id1 | {"object_field1": "value1", "object_field2_array": [{"array_object_field1": "array_object_value1"}]} | 10 |
    # | id2 | {"object_field1": "value2", "object_field2_array": [{"array_object_field1": "array_object_value1"}]} | 20 |
    # +---+------------------------------------------------------------------------------------------------+----+
    #
    # root
    # | -- id: string(nullable=true)
    # | -- json_string_col: string(nullable=true)
    # | -- col3: long(nullable=true)

    # Dynamically get struct type via spark.read.json
    json_schema = read_json_schema(spark, df)
    print(json_schema)
    print(type(json_schema))
    # StructType(List(StructField(object_field1,StringType,true),StructField(object_field2_array,ArrayType(StructType(List(StructField(array_object_field1,StringType,true))),true),true)))
    # <class 'pyspark.sql.types.StructType'>
    df_explode = from_json_explode(spark, df, json_schema)
    df_explode.show(3, False)
    df_explode.printSchema()

    # +---+------------------------------------------------------------------------------------------------+----+---------------------------------+---------------------+
    # |id |json_string_col                                                                                 |col3|json_col                         |json_explode         |
    # +---+------------------------------------------------------------------------------------------------+----+---------------------------------+---------------------+
    # |id1|{"object_field1":"value1","object_field2_array":[{"array_object_field1":"array_object_value1"}]}|10  |{value1, [{array_object_value1}]}|{array_object_value1}|
    # |id2|{"object_field1":"value2","object_field2_array":[{"array_object_field1":"array_object_value1"}]}|20  |{value2, [{array_object_value1}]}|{array_object_value1}|
    # +---+------------------------------------------------------------------------------------------------+----+---------------------------------+---------------------+
    #
    # root
    #  |-- id: string (nullable = true)
    #  |-- json_string_col: string (nullable = true)
    #  |-- col3: long (nullable = true)
    #  |-- json_col: struct (nullable = true)
    #  |    |-- object_field1: string (nullable = true)
    #  |    |-- object_field2_array: array (nullable = true)
    #  |    |    |-- element: struct (containsNull = true)
    #  |    |    |    |-- array_object_field1: string (nullable = true)
    #  |-- json_explode: struct (nullable = true)
    #  |    |-- array_object_field1: string (nullable = true)


if __name__ == '__main__':
    spark = SparkSession \
        .builder \
        .master("local") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    run_json_schema(spark)
