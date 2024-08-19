
import findspark

from pyspark.sql import SparkSession

from PythonHomeWork.spark_data_frame import data_frame_functional
from PythonHomeWork.spark_sql import spark_sql_functional

findspark.init()
findspark.find()


def main():
    spark=SparkSession.builder.appName("Currency exchange").getOrCreate()
    data_frame_functional(spark)
    spark_sql_functional(spark)
    spark.stop()

if __name__ == '__main__':
    main()