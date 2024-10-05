import pyspark.sql.types as t
from pyspark import SparkConf
from pyspark.sql import SparkSession

def basic_test_df():
    spark_session = (SparkSession.builder
                     .master("local")
                     .appName("task app")
                     .config(conf=SparkConf())
                     .getOrCreate())

    data = [('Volodymyr', 'Mishchenia', 'NaUKMA')]
    schema = t.StructType([
        t.StructField('first_name', t.StringType(), True),
        t.StructField('last_name', t.StringType(), True),
        t.StructField('university', t.StringType(), True)
    ])
    df = spark_session.createDataFrame(data=data, schema=schema)
    return df