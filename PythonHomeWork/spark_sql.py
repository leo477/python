from pyspark.sql import SparkSession

def spark_sql_functional(spark: SparkSession):
    df = spark.read.option("multiline", True).json("exchange-rate.json")
    df.createOrReplaceTempView("cc_view")
    query="select cc as currency_code, avg(rate) as average_rate from cc_view where cc in ('USD', 'EUR', 'CAD') group by cc"
    average_df=spark.sql(query)
    average_df.write.mode("overwrite").parquet("spark_sql_parquet/average_rates.parquet")