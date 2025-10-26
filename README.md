# Big Data Frameworks

# 1

# PySpark: DataFrames & Transformations

**File:** `pyspark_basics.py`

**Overview:**  
Basic PySpark examples showing DataFrame creation, schema, select, filter, column transformations, aggregations, union, and saving results as CSV.


# 2
# PySpark: Aggregations & Actions

**File:** `pyspark_aggregations.py`

**Overview:**  
Hands-on PySpark examples showing RDD transformations (`map`, `filter`, `flatMap`), pair-RDD operations (`reduceByKey`, `groupByKey`, `combineByKey`), and common actions (`collect`, `count`, `take`, `reduce`, `foreach`). Also demonstrates DataFrame aggregations using `groupBy().agg()` and persistence with `cache()` / `persist()`.

# 3
# Spark SQL Queries

**File**: spark_sql_queries.py

**Overview:**
Practical exploration of Spark SQL for data analysis. Covers creating Spark sessions, loading DataFrames, and registering temporary views to run SQL queries using spark.sql(). Demonstrates filtering, grouping, joining, and aggregating data through SQL syntax, along with combining SQL queries and DataFrame operations. Includes examples of saving query results for further processing or reporting.



# 4 
# DataFrame Joins & Window Functions

**File:** `day4_joins_windows.py`

**Overview:**  
Hands-on examples demonstrating how to merge DataFrames using different join types (inner, left, full outer) and how to apply window functions for analytical calculations. The script shows `rank`, `dense_rank`, `row_number`, `lag`, `lead`, and partitioned aggregates (e.g., max salary per department). It also includes a practical example to extract the top-2 paid employees per department.

# 5
# Spark SQL & Temporary Views

**File:** `day5_spark_sql.py`

**Overview:**  
Demonstrates how to query PySpark DataFrames using SQL syntax by registering them as temporary views. The script covers filtering, joins, aggregations, sorting, and using global temporary views. This bridges DataFrame APIs and SQL operations for more readable analytics workflows.

**Skills demonstrated:**
- Creating temporary & global views
- Running SQL queries using `spark.sql()`
- Performing joins, grouping, and filtering in SQL
- Integrating SQL-style queries with DataFrame transformations
  


# 6
# Spark Joins & DataFrame Relationships

**File:** `spark_joins.py`

**Overview:**  
Explores multiple types of joins in PySpark, including inner, left, right, full outer, semi, and anti joins. Demonstrates how to combine DataFrames based on matching keys, handle missing data, and understand which rows are preserved in each join type.

**Skills demonstrated:**
- Performing joins with `join()` on DataFrames
- Understanding join behavior and data relationships
- Handling nulls and missing records
- Using semi and anti joins for filtering operations







# 7
#  Spark + Kafka Integration

**File:** `spark_kafka_pipeline.py`

**Overview:**  
Implements a real-time streaming pipeline integrating Apache Spark Structured Streaming with Apache Kafka. The script reads live messages from a Kafka topic, performs transformations (like word count), and outputs the results to the console. Demonstrates the basics of end-to-end event streaming and data processing.

**Skills demonstrated:**
- Reading streaming data from Kafka with PySpark
- Transforming and aggregating data in real-time
- Writing continuous streaming results to console or sinks
- Understanding the Spark-Kafka connector




