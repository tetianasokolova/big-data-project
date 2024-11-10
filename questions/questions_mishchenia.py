import pyspark.sql.functions as f
from pyspark.sql import Window


def titles_with_rating_above_5(title_rating_df):
    return title_rating_df.filter(f.col('average_rating') > 5)


def titles_with_ukrainian_translation(title_akas_df):
    return title_akas_df.filter(f.col('region') == 'UA')


def william_dickson_films(title_crew_df):
    return title_crew_df.filter(f.col('directors') == 'nm0005690')


#1. Які актори знімалися в найбільшій кількості фільмів чи серіалів?
def actors_with_the_biggest_count_films(title_principals_df, actors_number):
    actors_df = title_principals_df.filter(
        (f.col("category") == "actor") | (f.col("category") == "actress"))

    return (actors_df.groupBy("nconst").agg(f.count_distinct("tconst"))
            .orderBy(f.col("count(DISTINCT tconst)").desc())
            .limit(actors_number))


# 17. Фільми якого жанру мають найдовшу cередню тривалість?
def genre_with_the_biggest_avg_film_time(title_basics_df, genres_count):
    filtered_movies = title_basics_df.filter(
        (f.col("runtime_minutes").isNotNull()) & (f.col("genres").isNotNull())
    )
    average_time_by_ganre = filtered_movies.groupBy("genres").agg(f.avg("runtime_minutes").alias("avg_duration"))
    return average_time_by_ganre.orderBy(f.col("avg_duration").desc()).limit(genres_count)


# 30. В якому регіоні найбільше перекладених філмів(окрім оригінальних)?
def region_with_the_biggest_translations(title_akas_df):
    filtered_df = title_akas_df.filter(
        (f.col("region") != "original_region")
    )

    translations_by_region = filtered_df.groupBy("region").agg(f.count("title_id").alias("translation_count"))
    return translations_by_region.orderBy(f.col("translation_count").desc()).limit(1)


# 31. Які фільми з високим середнім рейтингом (понад 8.0) мають найбільшу кількість голосів і займають топ-10 місць за кількістю голосів?

def top_highly_rated_movies_by_votes(title_ratings_df):
    high_rated_movies = title_ratings_df.filter((f.col("average_rating") > 8.0))

    window_spec = Window.orderBy(f.col("num_votes").desc())
    ranked_high_rated_movies = high_rated_movies.withColumn("rank", f.rank().over(window_spec))

    top_10_high_rated_movies = ranked_high_rated_movies.filter(f.col("rank") <= 10)
    return top_10_high_rated_movies


# 32. Які п'ять фільмів у кожному жанрі мають найвищу тривалість?
def top_5_longest_movies_by_genre(title_basics_df):
    filtered_movies = title_basics_df.filter((f.col("genres").isNotNull()))

    window = Window.partitionBy("genres").orderBy(f.col("runtime_minutes").desc())
    ranked_movies = filtered_movies.withColumn("row_number", f.row_number().over(window))

    return ranked_movies.filter(f.col("row_number") <= 5)

#40 Оригінальні назви 10 фільмів(не серіалу), які мають найвищий рейтинг і хоча б 100 голосів
def best_10_film_original_name(title_basic_df, title_ratings_df):
    movies= title_basic_df.filter((f.col('title_type')=='movie')).select(['tconst','original_title'])
    best_movie=(movies.join(title_ratings_df,on='tconst',how='left').filter(f.col('num_votes')>=100).
                orderBy('average_rating',ascending=False).limit(10))
    return best_movie

#20 Чи є зв'язок між кількістю епізодів у серіалі та його середнім рейтингом?
def relation_episodes_amount_and_rating(title_episode_df,title_ratings_df ):
    episodes_amount=title_episode_df.select(['tconst','parent_tconst',]).groupBy('parent_tconst').count()
    join_condition=(episodes_amount['parent_tconst']==title_ratings_df['tconst'])

    exploded_episodes_amount=episodes_amount.join(title_ratings_df,on=join_condition,how='inner')
    return exploded_episodes_amount.groupBy('count').agg({'average_rating':'avg'}).orderBy('count')

