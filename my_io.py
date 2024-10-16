from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.types as t

def read_name_basics_df(path):
    # create spark session
    spark_session = ((SparkSession.builder
                     .master("local")
                     .appName("task app"))
                     .config(conf=SparkConf())
                     .getOrCreate())

    # read the dataframe from file
    name_basics_df = spark_session.read.csv(path,
                                            sep="\t",
                                            header = True)
    return name_basics_df