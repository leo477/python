from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg


def data_frame_functional(spark: SparkSession):
    df = spark.read.option("multiline", True).json("exchange-rate.json").withColumnRenamed("cc", "currency_code")
    filter_values = ["USD", "EUR", "CAD"]
    filtered = df.filter(col("currency_code").isin(filter_values))
    result_df = filtered.groupBy("currency_code") \
        .agg(avg("currency_code").alias("rate")).withColumnRenamed("rate","average_rate")
    result_df.printSchema()
    result_df.write.mode("overwrite").parquet("data_frame_parquet/average_rate.parquet")