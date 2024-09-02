from datetime import datetime
import random
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, FloatType

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
    
    data = [(1,"John", 25), (40,"Mary", 30), (2,"Mike", 35), (3,"Mike", 35), (4,"Mike", 35), (5,"Mike", 35), (6,"Mike", 35), (7,"Mike", 35), (8,"Mike", 35), (9,"Mike", 35)
            , (10,"Mike", 35), (11,"Mike", 35), (12,"Mike", 35), (13,"Mike", 35), (14,"Mike", 35), (15,"Mike", 35), (16,"Mike", 35), (17,"Mike", 35), (18,"Mike", 35)]

    schema = StructType([
        StructField("account_id", IntegerType()),
        StructField("name", StringType()),
        StructField("age", IntegerType())
    ])

# Створення DataFrame з Row
    accounts_df = spark.createDataFrame(data,schema)
    s1_df= spark.read.parquet("s3://raw-data/transaction.parquet")
    joined_df = s1_df.join(accounts_df, s1_df.from_account == accounts_df.account_id, "inner")
    joined_df.write.parquet("s3a://raw-data/s2_df_join.parquet", mode="overwrite")
    joined_df.show()
    spark.stop()

if __name__ == "__main__":
    main()