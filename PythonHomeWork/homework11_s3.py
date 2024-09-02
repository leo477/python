from datetime import datetime
import random
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, FloatType
from pyspark.sql.functions import col, when

USD_to_UAH = 41.0
EUR_to_UAH = 42.0

def main():
    spark = SparkSession.builder \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", False) \
        .config("spark.hadoop.fs.s3a.path.style.access", True) \
        .config("spark.hadoop.fs.s3a.endpoint", "localstack:4566") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.access.key", "test") \
        .config("spark.hadoop.fs.s3a.secret.key", "test") \
        .getOrCreate()

    df= spark.read.parquet("s3://raw-data/s2_df_join.parquet")
    converted_df = df.withColumn(
        "amount_in_uah",
        when(col("currency") == "USD", col("amount") * USD_to_UAH)
        .when(col("currency") == "EUR", col("amount") * EUR_to_UAH)
        .otherwise(col("amount"))
)
    converted_df.write.parquet("s3a://raw-data/s3_df_join.parquet", mode="overwrite")
    spark.stop()

if __name__ == "__main__":
    main()