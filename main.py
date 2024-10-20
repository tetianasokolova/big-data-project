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

from setting import AKAS_DF_PATH, AKAS_DF_RESULTS_PATH, TITLE_CREW_DF_PATH, TITLE_CREW_DF_RESULTS_PATH

from my_io import read_akas_df, write_akas_df_to_csv, read_title_crew_df, write_title_crew_df_to_csv
from dfs_postprocess.title_akas_postprocess import title_akas_postprocess

akas_df = read_akas_df(AKAS_DF_PATH)
akas_df = title_akas_postprocess(akas_df)
write_akas_df_to_csv(akas_df,AKAS_DF_RESULTS_PATH)

title_crew_df = read_title_crew_df(TITLE_CREW_DF_PATH)


