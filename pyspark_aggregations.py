"""
 PySpark - Transformations & Actions


Covers:
- RDD transformations: map, filter, flatMap
- Pair RDD operations: mapToPair, reduceByKey, groupByKey
- Actions: collect, count, take, reduce, foreach
- DataFrame aggregations: groupBy().agg(), sum, avg, count
- Persist/cache example and performance note

"""

from pyspark.sql import SparkSession
from pyspark.storagelevel import StorageLevel
from pyspark.sql.functions import col, sum as _sum, avg as _avg, count as _count

def main():
    spark = SparkSession.builder \
        .appName("pyspark_aggregations_day2") \
        .master("local[*]") \
        .getOrCreate()

    sc = spark.sparkContext

    print("\n=== RDD: create from list ===")
    data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    rdd = sc.parallelize(data, 2)  # 2 partitions
    print("rdd.collect():", rdd.collect())
    print("rdd.count():", rdd.count())

    # -------------------------
    # Transformations: map, filter, flatMap
    # -------------------------
    print("\n=== Transformations: map, filter, flatMap ===")
    mapped = rdd.map(lambda x: x * 10)
    filtered = mapped.filter(lambda x: x > 30)
    # flatMap example: split sentences into words
    sentences = sc.parallelize(["hello world", "spark with python", "map filter reduce"])
    words = sentences.flatMap(lambda s: s.split(" "))
    print("mapped.collect():", mapped.collect())
    print("filtered.collect():", filtered.collect())
    print("words.collect():", words.collect())

    # -------------------------
    # Pair RDDs: (key, value) for reduceByKey and groupByKey
    # -------------------------
    print("\n=== Pair RDDs: reduceByKey vs groupByKey ===")
    purchases = [
        ("aravind", 100.0),
        ("rahul", 50.0),
        ("aravind", 200.0),
        ("priya", 75.0),
        ("rahul", 25.0),
        ("john", 80.0)
    ]
    purchases_rdd = sc.parallelize(purchases)

    # reduceByKey: combines values locally before shuffling (better)
    total_by_user_reduce = purchases_rdd.reduceByKey(lambda a, b: a + b)
    print("total_by_user_reduce.collect():", total_by_user_reduce.collect())

    # groupByKey: groups all values by key (can be more expensive)
    grouped = purchases_rdd.groupByKey().mapValues(list)
    print("grouped.collect():", grouped.collect())

    # Example: compute average per user using combineByKey (efficient)
    def create_combiner(value):
        return (value, 1)
    def merge_value(acc, value):
        return (acc[0] + value, acc[1] + 1)
    def merge_combiners(acc1, acc2):
        return (acc1[0] + acc2[0], acc1[1] + acc2[1])

    avg_by_user = purchases_rdd.combineByKey(create_combiner, merge_value, merge_combiners) \
        .mapValues(lambda acc: acc[0] / acc[1])
    print("avg_by_user.collect():", avg_by_user.collect())

    # -------------------------
    # Actions: take, reduce, foreach
    # -------------------------
    print("\n=== Actions: take, reduce, foreach ===")
    print("rdd.take(5):", rdd.take(5))
    print("rdd.reduce(sum):", rdd.reduce(lambda a, b: a + b))

    print("foreach example (print each purchase):")
    def print_purchase(x):
        print("purchase:", x)
    purchases_rdd.foreach(print_purchase)  # runs on workers; may not show in driver console reliably

    # -------------------------
    # DataFrame aggregations
    # -------------------------
    print("\n=== DataFrame aggregations ===")
    df_data = [
        (1, "Aravind", "UK", 100.0),
        (2, "Rahul", "UK", 50.0),
        (3, "Priya", "IN", 200.0),
        (4, "Sita", "IN", 150.0),
        (5, "John", "US", 80.0),
        (6, "Anita", "UK", 120.0)
    ]
    df = spark.createDataFrame(df_data, schema=["order_id", "customer", "country", "price"])
    df.show()
    df.printSchema()

    print("GroupBy country: sum(price), avg(price), count(*)")
    agg_df = df.groupBy("country").agg(
        _sum(col("price")).alias("total_price"),
        _avg(col("price")).alias("avg_price"),
        _count("*").alias("num_orders")
    )
    agg_df.show()

    # collect() action returns to driver
    results = agg_df.collect()
    print("Aggregations collected to driver:", results)

    # -------------------------
    # Caching/persistence example
    # -------------------------
    print("\n=== Persistence example ===")
    large_rdd = sc.parallelize(range(1, 1000001), 4).map(lambda x: x * 2)
    # cache because we'll query multiple times
    large_rdd.persist(StorageLevel.MEMORY_ONLY)
    print("large_rdd count:", large_rdd.count())
    print("large_rdd take(3):", large_rdd.take(3))
    large_rdd.unpersist()

    # -------------------------
    # Save a DataFrame aggregation result to CSV
    # -------------------------
    out_path = "output/pyspark_day2_agg"
    agg_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(out_path)
    print(f"Saved aggregation results to: {out_path}")

    spark.stop()


if __name__ == "__main__":
    main()
