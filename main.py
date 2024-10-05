from pyspark import SparkConf
from pyspark.sql import SparkSession
from basic_dfs import basic_df_stoliaruk

spark_session = (SparkSession.builder
                 .master("local")
                 .appName("task app")
                 .config(conf=SparkConf())
                 .getOrCreate())

basic_df_stoliaruk.basic_test_df().show()

spark_session.stop()