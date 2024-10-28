from pyspark import SparkConf
from pyspark.sql import SparkSession, Window
import pyspark.sql.types as t

def read_akas_df(path):
    spark_session = (SparkSession.builder
                     .master("local")
                     .appName("task app")
                     .config(conf=SparkConf())
                     .getOrCreate())
    akas_df_schema = t.StructType([
        t.StructField('titleId', t.StringType(), False),
        t.StructField('ordering', t.IntegerType(), False),
        t.StructField('title', t.StringType(), False),
        t.StructField('region', t.StringType(), True),
        t.StructField('language', t.StringType(), True),
        t.StructField('types', t.StringType(), True),
        t.StructField('attributes', t.StringType(), True),
        t.StructField('isOriginalTitle', t.IntegerType(), False)
        # при переведенні в boolean всюди простаавляються null тому залишаю Integer
    ])
    akas_df = spark_session.read.option('sep', '\t').csv(path, header=True, nullValue='null', schema=akas_df_schema)

    return akas_df


def read_title_crew_df(path):
    spark_session = (SparkSession.builder
                     .master("local")
                     .appName("task app")
                     .config(conf=SparkConf())
                     .getOrCreate())
    title_crew_df_schema = t.StructType([
        t.StructField('tconst', t.StringType(), nullable=False),
        t.StructField('directors', t.StringType(), False),
        t.StructField('writers', t.StringType(), True)])
    title_crew_df = spark_session.read.option('sep', '\t').csv(path, header=True, nullValue='null',
                                                               schema=title_crew_df_schema)

    return title_crew_df


def read_title_ratings_df(path):
    spark_session = (SparkSession.builder
                     .master("local")
                     .appName("task app")
                     .config(conf=SparkConf())
                     .getOrCreate())
    title_ratings_df_schema=t.StructType([
        t.StructField('tconst',t.StringType(),nullable=False),
        t.StructField('averageRating',t.DoubleType(),False),
        t.StructField('numVotes',t.IntegerType(),True)])
    title_ratings_df = spark_session.read.option('sep', '\t').csv(path,header=True,nullValue='null',schema=title_ratings_df_schema)

    return title_ratings_df


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
                                            nullValue="\\N",
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
                                            nullValue="\\N",
                                            schema=name_basics_df_schema)
    return name_basics_df


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


def read_title_episode_df(path):
    spark_session = (SparkSession.builder
                     .master("local")
                     .appName("task app")
                     .config(conf=SparkConf())
                     .getOrCreate())

    title_episode_df_schema = t.StructType([
        t.StructField('tconst', t.StringType(), True),
        t.StructField('parentTconst', t.StringType(), True),
        t.StructField('seasonNumber', t.IntegerType(), True),
        t.StructField('episodeNumber', t.IntegerType(), True)])

    title_episode_df = spark_session.read.option('sep', '\t').csv(path,
                                                                 header=True,
                                                                 nullValue='null',
                                                                 schema=title_episode_df_schema)
    return title_episode_df


def write_title_ratings_df_to_csv(df,path):
    df.write.csv(path,header=True,mode='overwrite',encoding='utf-8',sep=',')


def write_title_crew_df_to_csv(df, path):
    df.write.csv(path, header=True, mode='overwrite', encoding='utf-8', sep=',')


def write_akas_df_to_csv(df, path):
    df.write.csv(path, header=True, mode='overwrite', encoding='utf-8', sep=',')

# write title_principals_df
def write_title_principals_df_to_csv(df, path):
    df.write.csv(path, header=True, mode='overwrite', encoding='utf-8', sep=',')

# write name_basics_df
def write_name_basics_df_to_csv(df, path):
    df.write.csv(path, header=True, mode='overwrite', encoding='utf-8', sep=',')


def write_title_basics_df_to_csv(df, path):
    df.write.csv(path, sep=',', header=True, nullValue='null', mode='overwrite')


def write_title_episode_df_to_csv(df, path):
    df.write.csv(path, sep=',', header=True, nullValue='null', mode='overwrite')

def write_df_to_csv(df, path):
    df.write.csv(path, sep=',', header=True, nullValue='null', mode='overwrite')