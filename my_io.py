from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.types as t
import pyspark.sql.functions as f

def read_name_basics_df(path):
    # create spark session
    spark_session = ((SparkSession.builder
                     .master("local")
                     .appName("task app"))
                     .config(conf=SparkConf())
                     .getOrCreate())

    # create schema for dataframe name_basics_df
    name_basics_df_schema = t.StructType([t.StructField('nconst', t.StringType(), False),
                                          t.StructField('primaryName', t.StringType(), True),
                                          t.StructField('birthYear', t.IntegerType(), True),
                                          t.StructField('deathYear', t.IntegerType(), True),
                                          # in YYYY format if applicable, else '\N'
                                          t.StructField('primaryProfession', t.StringType(), True),  # array of strings
                                          t.StructField('knownForTitles', t.StringType(), True)
                                          # array of tconsts (tconst is of type string)
                                          ])

    # read the dataframe from file
    name_basics_df = spark_session.read.csv(path,
                                            sep="\t",
                                            header=True,
                                            nullValue='\\N',
                                            schema=name_basics_df_schema)

    # Transform the data in 'primaryProfession' and 'knownForTitles' from the comma-separated strings into arrays
    name_basics_df = (name_basics_df
                      .withColumn('primaryProfession', f.split(f.col("primaryProfession"), ","))
                      .withColumn('knownForTitles', f.split(f.col("knownForTitles"), ",")
                                  ))
    return name_basics_df