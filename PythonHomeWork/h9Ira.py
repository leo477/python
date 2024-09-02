import findspark

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg

findspark.init()
findspark.find()

def data_frame_functional(spark):
    # 1. Вичитати JSON файл
    df = spark.read.option("multiLine", True).json("exchange-rate.json")
    print("Схема DataFrame:")
    df.printSchema()
    print("Перші 5 рядків DataFrame:")
    df.show(5)

    # 2. Відфільтрувати потрібні валюти
    filtered_df = df.filter(df.cc.isin("USD", "EUR", "CAD"))

    print("Перші 5 рядків після фільтрації:")
    filtered_df.show(5)

    # 3. Обчислити середній курс по кожній валюті
    avg_df = filtered_df.groupBy("cc").agg(avg("rate").alias("average_rate"))

    # 4. Записати результат у форматі Parquet
    avg_df.write.mode("overwrite").parquet("output_parquet.parquet")
    spark.stop()


def spark_sql_functional(spark):
    # 1. Вичитати JSON файл
    df = spark.read.option("multiLine", True).json("exchange-rate.json")

    # Переглянути структуру та перші кілька рядків DataFrame
    print("Schema of DataFrame:")
    df.printSchema()

    print("First few rows of DataFrame:")
    df.show(5)  # Показати перші 5 рядків DataFrame

    df.createOrReplaceTempView("exchange_rates")

    # 2. Відфільтрувати потрібні валюти та обчислити середній курс
    result_df = spark.sql("""
        SELECT cc, AVG(rate) as average_rate
        FROM exchange_rates
        WHERE cc IN ('USD', 'EUR', 'CAD')
        GROUP BY cc
    """)
    print("First few rows of Result DataFrame:")
    result_df.show(5)

    # 3. Записати результат у форматі Parquet
    result_df.write.mode("overwrite").parquet("output_parquet_sql.parquet")



def main():
    spark = SparkSession.builder.appName("Currency exchange").getOrCreate()
    # Викликати обидві функції для виконання завдання
    data_frame_functional(spark)
    spark_sql_functional(spark)
    spark.stop()


if __name__ == '__main__':
    main()