import pyspark.sql.functions as f
from pyspark.sql.window import Window

# question 24: які унікальні ідентифікатори та рейтинги у фільмів, що мають більше як 10,000 голосів?
def title_rating_with_votes_above_10000(title_rating_df):
    return (title_rating_df.filter(f.col('num_votes') > 10000)
                           .select(['tconst', 'average_rating']))

# question 25: які основні професії Raquel Welch (nm0000079)?
def raquel_welch_primary_profession(name_basics_df):
    return (name_basics_df.filter(f.col('nconst') == 'nm0000079')
                          .select(['primary_name', 'primary_profession']))

# question 26: які унікальні ідентифікатори та локальні назви для фільмів з регіону
# 'DE' (Німеччина) або 'CH' (Швейцарія)?
def titles_with_german_or_swiss_region(akas_df):
    return (akas_df.filter(f.col('region').isin(['DE', 'CH']))
                   .select(['title_id', 'title', 'region']))

# aggregation, grouping and sorting questions
# question 3: скільки фільмів (title_type = 'movie') було випущено в кожен рік випуску?
def count_movies_per_year(title_basics_df):
    count_movies_per_year_df = title_basics_df.filter((f.col('title_type') == 'movie')
                                                       & f.col('start_year').isNotNull())
    count_movies_per_year_df = (count_movies_per_year_df.groupBy(['start_year'])
                                                        .agg(f.count('tconst').alias('movies_count'))
                                                        .orderBy('start_year', ascending=True))
    return count_movies_per_year_df

# question 4: які 5 основних професій є найпоширенішими серед всіх осіб у базі?
def three_popular_professions(name_basics_df):
    # split the primary_profession column and select it
    professions_df = (name_basics_df.withColumn('primary_profession',
                                                f.explode(f.col('primary_profession')))
                                    .select(f.col('primary_profession')))
    # compute the count for each profession
    profession_counts = professions_df.groupBy('primary_profession').count()
    three_popular_professions_df = (profession_counts.orderBy('count', ascending=False)
                                                     .select('primary_profession')
                                                     .limit(5))
    return three_popular_professions_df

# question 16: хто з режисерів працював над найбільшою кількістю фільмів?
def top_director_by_film_count(title_crew_df, name_basics_df):
    count_films_per_director = (title_crew_df.groupBy('directors')
                                             .count()
                                             .orderBy('count', ascending=False))
    top_director_by_film_count_df = (count_films_per_director.select('directors')
                                                             .filter(f.col('directors').isNotNull())
                                                             .limit(1))
    join_cond = (top_director_by_film_count_df['directors'] == name_basics_df['nconst'])
    top_director_by_film_count_df = (top_director_by_film_count_df.join(name_basics_df, on=join_cond, how='inner')
                                                                  .select('nconst', 'primary_name'))
    return top_director_by_film_count_df

# return number of null values for each column (it was used for cleaning)
def null_values_count(df):
    cols = df.columns
    null_values_count_df = df.select([f.count(f.when(f.col(c).isNull(), c))
                                       .alias(c) for c in cols])
    return null_values_count_df