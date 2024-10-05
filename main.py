from pyspark import SparkConf
from pyspark.sql import SparkSession
from basic_dfs.basic_df_stoliaruk import basic_test_df

spark_session = (SparkSession.builder
                 .master("local")
                 .appName("task app")
                 .config(conf=SparkConf())
                 .getOrCreate())
df = basic_test_df(spark_session)
df.show()g
spark_session.stop()