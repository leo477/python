from pyspark.sql import SparkSession
from pyspark.sql.functions import col, json_tuple, sum, count

def consumer():
    spark = SparkSession \
        .builder \
        .appName("kafka-consumer-app") \
        .master("local[*]") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2") \
        .getOrCreate()

    bootstrap_servers = "localhost:9092"

    topic_name = "Homework14"

    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", bootstrap_servers) \
        .option("subscribe", topic_name) \
        .option("minOffsetsPerTrigger", 100) \
        .option("maxTriggerDelay", "100s") \
        .load()

    json_extract_col = json_tuple("json_payload",
                                  "id",
                                  "from_account_id"
                                  )
    kafka_read_df = df.selectExpr("CAST(key AS STRING) AS transaction_id", "CAST(value AS STRING) as json_payload") \
        .select(col("transaction_id"), json_extract_col) \
        .select(col("transaction_id"), col("c0").alias("id"),
                col("c1").alias("from_account_id"))

    kafka_read_df.writeStream \
        .format("console") \
        .start() \
        .awaitTermination()


if __name__ == '__main__':
    consumer()