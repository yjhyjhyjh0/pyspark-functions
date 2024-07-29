# pyspark-functions
Record some pyspark calculation


# Example
D1-unit test example of pyspark  
D2-window function (pyspark_window.py)[pyspark_window.py](core%2Fpyspark_window.py)   
3-prase json string schema(pyspark_json_schema.py)[pyspark_json_schema.py](core%2Fpyspark_json_schema.py)   
4-unpivot [pyspark_melt.py](core%2Fpyspark_melt.py)  
5-lateral_view_json_tuple [pyspark_laterel_view_json_tuple.py](core%2Fpyspark_laterel_view_json_tuple.py)  


# Test
How to run unit test
```
venv/bin/python3  -m pytest test/simple_compute/test_simple_compute.py
```

# Config
```
spark.sql.adaptive.shuffle.targetPostShuffleInputSize, 67108864 
The target post-shuffle input size in bytes of a task. By default is (64 MB)

spark.sql.suffle.partitions, 400
the number of partitions that are used when shuffling data for joins or aggregations. 

spark.default.parallelism, 200
the default number of partitions in RDDs returned by transformations like join, reduceByKey.

spark.sql.adaptive.enabled, True
spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version, "2"
https://stackoverflow.com/questions/61364226/how-to-change-the-version-of-fileoutputcommitter-algorithm-in-pyspark

spark 3 new feature 
Support Hadoop3/JDK 11/ Scala2.12
1-adaptive query execution
2-dynamic partition pruning
3-accelerator aware scheduler
4-ANSI SQL compilance
https://www.databricks.com/blog/2020/06/18/introducing-apache-spark-3-0-now-available-in-databricks-runtime-7-0.html
spark3.2.0 support zstd builtin


1-adaptive query execution
spark.sql.adaptive.enabled
-> join optimize -> auto optimize into broadcast join
-> coalesce shuffle partition 
-> skew join optimization

2-dynamic partition pruning
spark.sql.optimizer.dynamicPartitionPruning.enabled
-> join on partition key will optimize
3-accelerator aware scheduler
4-ANSI SQL compilance

```

# FAQ
Q1- How to set log level of spark at runtime
```
spark.sparkContext.setLogLevel("ERROR")
```

