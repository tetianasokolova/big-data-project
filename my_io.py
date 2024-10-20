from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.types as t

# read title_principals_df
def read_title_principals_df(path):
    # create spark session
    spark_session = ((SparkSession.builder
                     .master("local")
                     .appName("task app"))
                     .config(conf=SparkConf())
                     .getOrCreate())

    # create schema for dataframe title_principals_df
    title_principals_df_schema = t.StructType([t.StructField('tconst', t.StringType(), False),
                                          t.StructField('ordering', t.IntegerType(), True),
                                          t.StructField('nconst', t.StringType(), True),
                                          t.StructField('category', t.StringType(), True),
                                          t.StructField('job', t.StringType(), True), # '\N' for missed values
                                          t.StructField('characters', t.StringType(), True) # '\N' for missed values
                                          ])

    # read the dataframe from file
    name_basics_df = spark_session.read.csv(path,
                                            sep="\t",
                                            header=True,
                                            schema=title_principals_df_schema)
    return name_basics_df

# read name_basics_df
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
                                            schema=name_basics_df_schema)
    return name_basics_df

# write name_basics_df
def write_name_basics_df_to_csv(df, path):
    df.write.csv(path, header=True, mode='overwrite', encoding='utf-8', sep=',')