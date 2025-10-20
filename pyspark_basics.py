"""
Day 1: PySpark Basics - DataFrames & Transformations
Author: Aravind Amruth Madanu

What this script does:
- Creates a sample dataset (in-memory)
- Demonstrates DataFrame creation, schema, select, filter, withColumn, drop, union
- Shows column transformations, UDF usage, grouping, and saving output as CSV
- Print representative outputs to console (show, count, schema)
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, expr, lit, upper
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

def main():
    # 1) Create Spark session
    spark = SparkSession.builder \
        .appName("pyspark_basics_day1") \
        .master("local[*]") \
        .getOrCreate()

    # 2) Define schema and sample data
    schema = StructType([
        StructField("order_id", IntegerType(), False),
        StructField("customer", StringType(), False),
        StructField("quantity", IntegerType(), False),
        StructField("price", DoubleType(), False),
        StructField("country", StringType(), True)
    ])

    data = [
        (1, "Aravind", 2, 100.0, "UK"),
        (2, "Rahul", 5, 50.0, "UK"),
        (3, "Priya", 1, 200.0, "IN"),
        (4, "Sita", 3, 150.0, "IN"),
        (5, "John", 4, 80.0, "US")
    ]

    # 3) Create DataFrame
    df = spark.createDataFrame(data, schema=schema)
    print("=== Original DataFrame ===")
    df.show()
    df.printSchema()

    # 4) Basic selects and expressions
    print("=== Select with computed column (total_price) ===")
    df_with_total = df.withColumn("total_price", col("quantity") * col("price"))
    df_with_total.select("order_id", "customer", "quantity", "price", "total_price").show()

    # 5) Filtering rows
    print("=== Filter: total_price > 300 ===")
    df_with_total.filter(col("total_price") > 300).show()

    # 6) Add a new column using when/otherwise
    df_flagged = df_with_total.withColumn(
        "high_value",
        when(col("total_price") >= 300, lit(True)).otherwise(lit(False))
    )
    print("=== DataFrame with high_value flag ===")
    df_flagged.show()

    # 7) Transformations: uppercase customer names & country code to full name mapping
    df_transformed = df_flagged.withColumn("customer_upper", upper(col("customer"))) \
        .withColumn("country_full",
                    when(col("country") == "UK", lit("United Kingdom"))
                    .when(col("country") == "IN", lit("India"))
                    .when(col("country") == "US", lit("United States"))
                    .otherwise(lit("Unknown"))
                   )

    print("=== Transformed DataFrame (customer_upper, country_full) ===")
    df_transformed.select("order_id", "customer_upper", "country", "country_full").show()

    # 8) Aggregations and groupBy
    print("=== Aggregation: total sales per country ===")
    sales_by_country = df_with_total.groupBy("country") \
        .agg({"total_price": "sum", "order_id": "count"}) \
        .withColumnRenamed("sum(total_price)", "sum_total_price") \
        .withColumnRenamed("count(order_id)", "num_orders")
    sales_by_country.show()

    # 9) Union example - create second DataFrame and union
    new_data = [
        (6, "Anita", 2, 120.0, "UK"),
        (7, "Kumar", 1, 300.0, "IN")
    ]
    df2 = spark.createDataFrame(new_data, schema=schema)
    combined = df.union(df2)
    print("=== Combined DataFrame (after union) ===")
    combined.show()

    # 10) Save output (CSV) to local folder ./output/pyspark_day1 (overwrites if exists)
    out_path = "output/pyspark_day1"
    # remove if exists (safe for local), Spark will overwrite if mode=overwrite
    combined.write.mode("overwrite").option("header", "true").csv(out_path)
    print(f"Saved combined DataFrame to: {out_path}")

    # 11) Count rows and stop
    print("Total rows in combined:", combined.count())

    spark.stop()

if __name__ == "__main__":
    main()
