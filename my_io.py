from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.types as t

def read_title_basics_df(path):
    spark_session = (SparkSession.builder
                     .master("local")
                     .appName("task app")
                     .config(conf=SparkConf())
                     .getOrCreate())

    title_basics_df_schema = t.StructType([
        t.StructField('tconst', t.StringType(), True),
        t.StructField('titleType', t.StringType(), True),
        t.StructField('primaryTitle', t.StringType(), True),
        t.StructField('originalTitle', t.StringType(), True),
        t.StructField('isAdult', t.IntegerType(), True),
        t.StructField('startYear', t.IntegerType(), True),
        t.StructField('endYear', t.IntegerType(), True),
        t.StructField('runtimeMinutes', t.IntegerType(), True),
        t.StructField('genres', t.StringType(), True) ])

    title_basics_df = spark_session.read.option('sep', '\t').csv(path,
                                                                 header=True,
                                                                 nullValue='null',
                                                                 schema=title_basics_df_schema)
    return title_basics_df


def write_title_basics_df_to_csv(df, path):
    df.write.csv(path, sep=',', header=True, nullValue='null', mode='overwrite')

