from pyspark import SparkConf
from pyspark.sql import SparkSession, Window
import pyspark.sql.types as t
import pyspark.sql.functions as f


def read_akas_df(path):
    spark_session = (SparkSession.builder
                     .master("local")
                     .appName("task app")
                     .config(conf=SparkConf())
                     .getOrCreate())
    akas_df_schema=t.StructType([
        t.StructField('titleId',t.StringType(),False),
        t.StructField('ordering',t.IntegerType(),False),
        t.StructField('title',t.StringType(),False),
        t.StructField('region',t.StringType(),True),
        t.StructField('language',t.StringType(),True),
        t.StructField('types',t.StringType(),True),
        t.StructField('attributes',t.StringType(),True),
        t.StructField('isOriginalTitle',t.IntegerType(),False) #при переведенні в boolean всюди простаавляються null тому залишаю Integer
                                 ])
    akas_df = spark_session.read.option('sep', '\t').csv(path,header=True,nullValue='null',schema=akas_df_schema)

    return akas_df

def write_akas_df_to_csv(df,path):
    df.write.csv(path,header=True,mode='overwrite',encoding='utf-8',sep=',',emptyValue='nan')