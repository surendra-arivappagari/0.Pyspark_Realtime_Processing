# 0.Pyspark_Realtime_Processing
0.Pyspark_Realtime_Processing

# PySpark Optimization techniques:

1. Use select to Avoid Unnecessary Columns
Use Case: Minimize data transfer and memory usage by selecting only required columns.
python
Copy code
df = spark.createDataFrame([(1, "Alice", 25), (2, "Bob", 30)], ["id", "name", "age"])
df_selected = df.select("name", "age")
df_selected.show()


2. Filter Early (Push Down Filters)
Use Case: Reduce the size of data processed by filtering early.
python
Copy code
df_filtered = df.filter(df.age > 25)
df_filtered.show()


3. Use Broadcast Joins for Small Tables
Use Case: Improve join performance by broadcasting a smaller table.
python
Copy code
small_df = spark.createDataFrame([(1, "USA"), (2, "UK")], ["id", "country"])
large_df = spark.createDataFrame([(1, "Alice", 25), (2, "Bob", 30)], ["id", "name", "age"])

from pyspark.sql.functions import broadcast
df_joined = large_df.join(broadcast(small_df), "id")
df_joined.show()


4. Cache/Persist for Reused DataFrames
Use Case: Cache intermediate results for reuse in multiple actions.
python
Copy code
df_cached = df.filter(df.age > 25).cache()
print(df_cached.count())  # Trigger caching


5. Use coalesce for Reducing Partitions
Use Case: Reduce the number of partitions to avoid small tasks.
python
Copy code
df_repartitioned = df.coalesce(1)


6. Avoid Count in Large DataFrames
Use Case: Replace unnecessary count() with limit() when testing.
python
Copy code
df.limit(5).show()


7. Leverage Window Functions
Use Case: Efficiently perform ranking or aggregation over partitions.
python
Copy code
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

window_spec = Window.partitionBy("age").orderBy("name")
df_window = df.withColumn("row_num", row_number().over(window_spec))
df_window.show()



8. Use Built-in Functions
Use Case: Use optimized Spark functions over Python UDFs.
python
Copy code
from pyspark.sql.functions import upper
df_upper = df.withColumn("name_upper", upper(df.name))
df_upper.show()


9. Use explain to Analyze Queries
Use Case: Debug and optimize query execution plans.
python
Copy code
df_filtered.explain()


10. Use Partitioning for Skewed Data
Use Case: Write partitioned data for efficient reads.
python
Copy code
df.write.partitionBy("age").parquet("output_path")


11. Avoid Collecting Large Data
Use Case: Prevent memory bottlenecks in the driver.
python
Copy code
df.limit(10).toPandas()  # For small data only


12. Use .repartition() for Load Balancing
Use Case: Repartition data for better parallelism in large datasets.
python
Copy code
df_repartitioned = df.repartition(4)


13. Enable Dynamic Partition Pruning
Use Case: Optimize partitioned queries.
python
Copy code
spark.conf.set("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true")


14. Avoid Shuffling with groupByKey
Use Case: Use reduceByKey or agg to minimize shuffle.
python
Copy code
rdd = spark.sparkContext.parallelize([("a", 1), ("b", 2), ("a", 3)])
rdd_reduced = rdd.reduceByKey(lambda x, y: x + y)


15. Use Efficient File Formats (Parquet/ORC)
Use Case: Read/write in optimized file formats.
python
Copy code
df.write.parquet("output_path")


16. Vectorized UDFs for Performance
Use Case: Use pandas_udf for high-performance user-defined functions.
python
Copy code
import pandas as pd
from pyspark.sql.functions import pandas_udf

@pandas_udf("int")
def add_one(x: pd.Series) -> pd.Series:
    return x + 1

df = df.withColumn("age_plus_one", add_one(df.age))



17. Avoid explode for Large Arrays
Use Case: Use map or alternative structures when possible.
python
Copy code
from pyspark.sql.functions import explode
df_arrays = spark.createDataFrame([([1, 2, 3],)], ["nums"])
df_exploded = df_arrays.withColumn("num", explode(df_arrays.nums))
df_exploded.show()


18. Enable Predicate Pushdown
Use Case: Push filtering to the data source.
python
Copy code
spark.conf.set("spark.sql.parquet.filterPushdown", "true")
df_filtered = spark.read.parquet("data.parquet").filter("age > 25")


19. Use Broadcast Variables for Small Data
Use Case: Efficiently share small datasets with workers.
python
Copy code
broadcast_var = spark.sparkContext.broadcast([1, 2, 3])


20. Tune Shuffle Partitions
Use Case: Adjust default shuffle partitions for optimal parallelism.
python
Copy code
spark.conf.set("spark.sql.shuffle.partitions", "200")