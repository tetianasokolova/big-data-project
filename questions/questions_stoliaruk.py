import pyspark.sql.functions as f

# 27 Скільки епізодів у серіалі "Twin Peaks" 1990 року?
def episodes_of_twin_peaks_1990(title_episode_df):
    return title_episode_df.filter(f.col("parent_tconst") == 'tt0098936').count()

# 28 Які фільми тривають більше двох годин?
def more_than_2_hours_long_films(title_basics_df):
    return title_basics_df.filter(f.col('runtime_minutes')>120)

# 29. Які фільми дозволено дивитись дітям?
def children_friendly_films(title_basics_df):
    return title_basics_df.filter(f.col('is_adult')==False)

#8 Коли випускали найбільше фільмів?
def most_released_years(title_basics_df):
    return title_basics_df.filter((f.col("start_year").isNotNull())).groupBy(f.col('start_year')).count().orderBy('count', ascending=False)