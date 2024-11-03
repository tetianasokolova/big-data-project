import pyspark.sql.functions as f


def titles_with_rating_above_5(title_rating_df):
    return title_rating_df.filter(f.col('average_rating') > 5)


def titles_with_ukrainian_translation(title_akas_df):
    return title_akas_df.filter(f.col('region') == 'UA')


def william_dickson_films(title_crew_df):
    return title_crew_df.filter(f.col('directors') == 'nm0005690')


# Які актори знімалися в найбільшій кількості фільмів чи серіалів?
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
