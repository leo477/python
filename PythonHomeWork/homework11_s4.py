from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum

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

    df= spark.read.parquet("s3://raw-data/s3_df_join.parquet")
    df = df.withColumn("amount_in_uah", col("amount_in_uah").cast("double"))
    df.show()
    
    grouped_df=df.groupBy("date", "account_id").agg(sum("amount_in_uah").alias("money_amount_sent"))
    grouped_df.show()
    grouped_df.write.parquet("s3a://raw-data/s4_df_join.parquet", mode="overwrite")
    spark.stop()

if __name__ == "__main__":
    main()