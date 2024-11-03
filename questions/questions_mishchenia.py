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


