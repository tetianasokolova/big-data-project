from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.types as t

def basic_test_df():
    spark_session = (SparkSession.builder
                     .master("local")
                     .appName("task app")
                     .config(conf=SparkConf())
                     .getOrCreate())

    data = [('Tetiana', 'Mathematics', 94.3),
            ('Anastasia', 'Informatics', 85.7),
            ('Ivan', 'Cybersecurity', 96.8)]
    schema = t.StructType([
        t.StructField('name', t.StringType(), False),
        t.StructField('major', t.StringType(), True),
        t.StructField('GPA', t.DoubleType(), True)
    ])
    df = spark_session.createDataFrame(data=data, schema=schema)
    return df