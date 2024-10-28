import pyspark.sql.functions as f

# 27 How many films are in Twin Peaks 1990?
def episodes_of_twin_peaks_1990(title_episode_df):
    return title_episode_df.filter(f.col("parent_tconst") == 'tt0098936').count()

# 28 What films last longer than 2 hours?
def more_than_2_hours_long_films(title_basics_df):
    return title_basics_df.filter(f.col('runtime_minutes')>120)

# 29 What films are children allowed to watch?
def children_friendly_films(title_basics_df):
    return title_basics_df.filter(f.col('is_adult')==False)