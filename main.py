from pyspark import SparkConf
from pyspark.sql import SparkSession
from basic_dfs.basic_df_mishcenia import basic_test_df

spark_session = (SparkSession.builder
                 .master("local")
                 .appName("task app")
                 .config(conf=SparkConf())
                 .getOrCreate())

basic_test_df().show()

spark_session.stop()