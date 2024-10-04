from pyspark import SparkConf
from pyspark.sql import SparkSession

spark_session = (SparkSession.builder
                 .master("local")
                 .appName("task app")
                 .config(conf=SparkConf())
                 .getOrCreate())

spark_session.stop()