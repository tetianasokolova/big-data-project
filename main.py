# from pyspark import SparkConf
# from pyspark.sql import SparkSession
# from basic_dfs import basic_df_stoliaruk
# from basic_dfs import basic_df_sokolova
# from basic_dfs import basic_df_mishcenia
#
# spark_session = (SparkSession.builder
#                  .master("local")
#                  .appName("task app")
#                  .config(conf=SparkConf())
#                  .getOrCreate())
#
# basic_df_stoliaruk.basic_test_df().show()
# basic_df_sokolova.basic_test_df().show()
# basic_df_mishcenia.basic_test_df().show()
# spark_session.stop()

from setting import AKAS_DF_PATH, AKAS_DF_RESULTS_PATH, TITLE_CREW_DF_PATH, TITLE_CREW_DF_RESULTS_PATH, \
    TITLE_RATINGS_DF_RESULTS_PATH, TITLE_RATINGS_DF_PATH
from setting import (TITLE_PRINCIPALS_DF_PATH, TITLE_PRINCIPALS_RESULTS_PATH,
                     NAME_BASICS_DF_PATH, NAME_BASICS_DF_RESULTS_PATH)

from my_io import read_akas_df, write_akas_df_to_csv, read_title_crew_df, write_title_crew_df_to_csv, \
    read_title_ratings_df, write_title_ratings_df_to_csv
from my_io import (read_title_principals_df, write_title_principals_df_to_csv,
                   read_name_basics_df, write_name_basics_df_to_csv)

from dfs_postprocess.title_akas_postprocess import title_akas_postprocess
from dfs_postprocess.title_crew_postprocess import title_crew_postprocess
from dfs_postprocess.title_raitings_postprocess import title_rating_postprocess
from dfs_postprocess.name_basics_postprocess import name_basics_postprocess
from dfs_postprocess.title_principals_postprocess import title_principals_postprocess

akas_df = read_akas_df(AKAS_DF_PATH)
akas_df = title_akas_postprocess(akas_df)
write_akas_df_to_csv(akas_df, AKAS_DF_RESULTS_PATH)

title_crew_df = read_title_crew_df(TITLE_CREW_DF_PATH)
title_crew_df = title_crew_postprocess(title_crew_df)
write_title_crew_df_to_csv(title_crew_df, TITLE_CREW_DF_RESULTS_PATH)

title_ratings_df = read_title_ratings_df(TITLE_RATINGS_DF_PATH)
title_ratings_df = title_rating_postprocess(title_ratings_df)
write_title_ratings_df_to_csv(title_ratings_df, TITLE_RATINGS_DF_RESULTS_PATH)

title_principals_df = read_title_principals_df(TITLE_PRINCIPALS_DF_PATH)
title_principals_df = title_principals_postprocess(title_principals_df)
write_title_principals_df_to_csv(title_principals_df, TITLE_PRINCIPALS_RESULTS_PATH)

name_basics_df = read_name_basics_df(NAME_BASICS_DF_PATH)
name_basics_df = name_basics_postprocess(name_basics_df)
write_name_basics_df_to_csv(name_basics_df, NAME_BASICS_DF_RESULTS_PATH)