#from pyspark.sql import SparkSession
#import sys
#
#def main():
#    spark = SparkSession.builder \
#        .appName("SparkToLocalStackS3Job") \
#        .getOrCreate()
#
#    # Read data from LocalStack S3
#
#    # Process data (example: select all columns)
#    df = spark.createDataFrame(
#        [
#            ("sue", 32),
#            ("li", 3),
#            ("bob", 75),
#            ("heo", 13),
#        ],
#    [   "first_name", "age"],
#    )
#    df.show()
#    spark.stop()
#
#if __name__ == "__main__":
#    main()
from datetime import datetime
import random
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, FloatType


def generate_transaction():
    num_accounts = 100
    transaction_id = random.randint(1000, 9999)
    from_account = random.randint(1, num_accounts)
    to_account = random.randint(1, num_accounts)
    amount = round(random.uniform(10, 1000), 2)
    currency = random.choice(['USD', 'EUR', 'GBP', 'UAH'])
    date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    return transaction_id, from_account, to_account, amount, currency, date

def main():
    num_transactions = 1000
    transactions = [generate_transaction() for _ in range(num_transactions)]
    print('1')
    spark = SparkSession.builder \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", False) \
        .config("spark.hadoop.fs.s3a.path.style.access", True) \
        .config("spark.hadoop.fs.s3a.endpoint", "localstack:4566") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.access.key", "test") \
        .config("spark.hadoop.fs.s3a.secret.key", "test") \
        .getOrCreate()
    schema = StructType([
        StructField("transaction_id", IntegerType(), nullable=False),
        StructField("from_account", IntegerType(), nullable=False),
        StructField("to_account", IntegerType(), nullable=False),
        StructField("amount", FloatType(), nullable=False),
        StructField("currency", StringType(), nullable=False),
        StructField("date", StringType(), nullable=False)
    ])

    df = spark.createDataFrame(transactions,schema)
    filtered_df = df.filter(df['currency'].isin(['USD', 'UAH', 'EUR']))
    filtered_df.show()
    filtered_df.write.mode('overwrite').parquet("s3a://raw-data/transaction.parquet")
    spark.stop()

if __name__ == "__main__":
    main()