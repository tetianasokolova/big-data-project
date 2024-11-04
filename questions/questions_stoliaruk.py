import pyspark.sql.functions as f
from pyspark.sql import Window

# 27. Скільки епізодів у серіалі "Twin Peaks" 1990 року?
def episodes_of_twin_peaks_1990(title_episode_df):
    return title_episode_df.filter(f.col("parent_tconst") == 'tt0098936').count()

# 28. Які фільми тривають більше двох годин?
def more_than_2_hours_long_films(title_basics_df):
    return title_basics_df.filter(f.col('runtime_minutes')>120)

# 29. Які фільми дозволено дивитись дітям?
def children_friendly_films(title_basics_df):
    return title_basics_df.filter(f.col('is_adult')==False)

# 8. Коли випускали найбільше фільмів?
def most_released_years(title_basics_df):
    return title_basics_df.filter((f.col("start_year").isNotNull()))\
        .groupBy(f.col('start_year')).count().\
        orderBy('count', ascending=False)

# 35. Скільки фільмів (чи інших типів) кожного жанру випускались кожного року?
def genre_count_per_year(title_basics_df):
    return title_basics_df.filter((f.col("start_year").isNotNull()) & (f.col("genres").isNotNull()))\
        .groupBy("start_year", "genres") \
        .count() \
        .orderBy("start_year", ascending=False)

# 36. Скільки фільмів мають кожен цілий рейтинг?
def rating_count(title_ratings_df):
  title_ratings_df = title_ratings_df.withColumn("average_rating", f.round(f.col("average_rating")).cast("integer"))
  return title_ratings_df.groupBy("average_rating") \
      .count() \
      .orderBy("average_rating")

# 37. Яка середня тривалість фільмів кожного року?
def average_movies_runtime_per_year(title_basics_df):
    window_spec = Window.partitionBy(f.col("start_year")).orderBy(f.col("runtime_minutes").desc())
    movies_df = title_basics_df.filter((f.col('title_type') == 'movie') & (f.col("start_year").isNotNull()))
    average_runtime_df = movies_df.withColumn("average_runtime",
                                              f.round(f.avg("runtime_minutes").over(window_spec), 1)) \
        .select("start_year", "average_runtime")
    return average_runtime_df

# 38. Яка мінімальна, максимальна та середня тривалість фільмів для кожного типу (titleType)?
def duration_stats_per_type(title_basics_df):
    window_spec = Window.partitionBy(f.col("title_type")).orderBy(f.col("title_type"))
    duration_stats_df = title_basics_df \
        .withColumn("min_runtime", f.min("runtime_minutes").over(window_spec)) \
        .withColumn("max_runtime", f.max("runtime_minutes").over(window_spec)) \
        .withColumn("average_runtime", f.round(f.avg("runtime_minutes").over(window_spec), 1)) \
        .select("title_type", "min_runtime", "max_runtime", "average_runtime") \
        .distinct()
    return duration_stats_df