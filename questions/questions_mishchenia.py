import pyspark.sql.functions as f
from pyspark.sql import Window


def titles_with_rating_above_5(title_rating_df):
    return title_rating_df.filter(f.col('average_rating') > 5)


def titles_with_ukrainian_translation(title_akas_df):
    return title_akas_df.filter(f.col('region') == 'UA')


def william_dickson_films(title_crew_df):
    return title_crew_df.filter(f.col('directors') == 'nm0005690')


# 1. Which actors have starred in the greatest number of movies or TV shows?
def actors_with_the_biggest_count_films(title_principals_df, actors_number):
    actors_df = title_principals_df.filter(
        (f.col("category") == "actor") | (f.col("category") == "actress"))

    return (actors_df.groupBy("nconst").agg(f.count_distinct("tconst"))
            .orderBy(f.col("count(DISTINCT tconst)").desc())
            .limit(actors_number))


# 17. Movies of which genre have the longest duration?
def genre_with_the_biggest_avg_film_time(title_basics_df, genres_count):
    filtered_movies = title_basics_df.filter(
        (f.col("runtime_minutes").isNotNull()) & (f.col("genres").isNotNull())
    )
    average_time_by_ganre = filtered_movies.groupBy("genres").agg(f.avg("runtime_minutes").alias("avg_duration"))
    return average_time_by_ganre.orderBy(f.col("avg_duration").desc()).limit(genres_count)


# 30. In which region are there the most translated (non-original) movies?
def region_with_the_biggest_translations(title_akas_df):
    filtered_df = title_akas_df.filter(
        (f.col("region") != "original_region")
    )

    translations_by_region = filtered_df.groupBy("region").agg(f.count("title_id").alias("translation_count"))
    return translations_by_region.orderBy(f.col("translation_count").desc()).limit(1)


# 31. Which movies with a high average rating (over 8.0) have the largest number of votes and rank in the top 10 by vote count?
def top_highly_rated_movies_by_votes(title_ratings_df):
    high_rated_movies = title_ratings_df.filter((f.col("average_rating") > 8.0))

    window_spec = Window.orderBy(f.col("num_votes").desc())
    ranked_high_rated_movies = high_rated_movies.withColumn("rank", f.rank().over(window_spec))

    top_10_high_rated_movies = ranked_high_rated_movies.filter(f.col("rank") <= 10)
    return top_10_high_rated_movies


# 32. What are the top five longest movies in each genre?
def top_5_longest_movies_by_genre(title_basics_df):
    filtered_movies = title_basics_df.filter((f.col("genres").isNotNull()))

    window = Window.partitionBy("genres").orderBy(f.col("runtime_minutes").desc())
    ranked_movies = filtered_movies.withColumn("row_number", f.row_number().over(window))

    return ranked_movies.filter(f.col("row_number") <= 5)