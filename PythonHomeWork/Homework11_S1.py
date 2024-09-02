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
        .appName("SparkToLocalStackS3") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.2,com.amazonaws:aws-java-sdk-pom:1.12.365") \
        .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') \
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:4566") \
        .config("spark.hadoop.fs.s3a.access.key", "test") \
        .config("spark.hadoop.fs.s3a.secret.key", "test") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .getOrCreate()
    spark.conf.set("spark.sql.parquet.compression.codec", "gzip")
    schema = StructType([
        StructField("transaction_id", IntegerType(), nullable=False),
        StructField("from_account", StringType(), nullable=False),
        StructField("to_account", IntegerType(), nullable=False),
        StructField("amount", FloatType(), nullable=False),
        StructField("currency", StringType(), nullable=False),
        StructField("date", StringType(), nullable=False)
    ])

    df = spark.createDataFrame(transactions,schema)
    filtered_df = df.filter(df['currency'].isin(['USD', 'UAH', 'EUR']))
    filtered_df.show()
    filtered_df.write.mode('overwrite').parquet("s3a://raw-data/transactions.parquet")
    spark.stop()

if __name__ == "__main__":
    main()