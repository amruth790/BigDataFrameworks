"""
 Spark + Kafka Integration


Covers:
- Reading streaming data from Kafka topic
- Writing processed data back to console or Kafka
- Using Structured Streaming with PySpark
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, col

def main():
    spark = SparkSession.builder \
        .appName("Day7_Spark_Kafka_Pipeline") \
        .master("local[*]") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    
    # 1. Read Stream from Kafka
    
    kafka_bootstrap = "localhost:9092"
    topic_name = "input_topic"

    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap) \
        .option("subscribe", topic_name) \
        .option("startingOffsets", "latest") \
        .load()

    # Kafka data is in binary, so we cast to string
    df_parsed = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

    print("\n=== Incoming Kafka Stream ===")
    df_parsed.printSchema()

    
    # 2. Process the Stream
    
    # Example: simple word count from Kafka messages
    from pyspark.sql.functions import explode, split

    words = df_parsed.select(
        explode(split(col("value"), " ")).alias("word")
    )

    word_counts = words.groupBy("word").count()

    
    # 3. Output Stream
    
    query = word_counts.writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("truncate", "false") \
        .start()

    query.awaitTermination()


if __name__ == "__main__":
    main()
