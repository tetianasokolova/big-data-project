import pyspark.sql.functions as f

# 27
def episodes_of_twin_peaks_1990(title_episode_df):
    return title_episode_df.filter(f.col("parent_tconst") == 'tt0098936').count()


#28
def more_than_2_hours_long_films(title_basics_df):
    return title_basics_df.filter(f.col('runtime_minutes')>120)
