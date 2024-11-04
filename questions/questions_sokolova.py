import pyspark.sql.functions as f
from pyspark.sql.window import Window

# filtering questions
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

# window functions
# question 33: яка тривалість найдовшого фільму для кожного формату та
# яка різниця між тривалістю кожного фільму та тривалістю найдовшого фільму цього ж формату
# (можливі формати: 'movie', 'short', 'tvseries' etc)?
def runtime_diff_within_title_type(title_basics_df):
    window_spec = Window.partitionBy('title_type').orderBy(f.col('runtime_minutes').desc())
    # create column with max runtime within each title_type
    runtime_within_type_df = title_basics_df.withColumn('max_runtime_within_type',
                                                        f.max(f.col('runtime_minutes')).over(window_spec))
    # create column with difference between max runtime and runtime of movie within each title_type
    runtime_within_type_df = runtime_within_type_df.withColumn('diff_runtime_within_type',
                                                               f.col('max_runtime_within_type') - f.col('runtime_minutes'))
    # select columns that we need
    runtime_within_type_df = runtime_within_type_df.select('tconst', 'title_type','primary_title',
                                                           'max_runtime_within_type', 'diff_runtime_within_type')
    return runtime_within_type_df

# question 34: який ранг за тривалістю фільму має кожен фільм у межах свого року випуску?
def rank_by_runtime_within_start_year(title_basics_df):
    window_spec = Window.partitionBy('start_year').orderBy(f.col('runtime_minutes').desc())
    rank_by_runtime_df = title_basics_df.withColumn('rank_runtime_within_year',
                                                    f.dense_rank().over(window_spec))
    rank_by_runtime_df = rank_by_runtime_df.select('tconst','primary_title', 'start_year',
                                                   'runtime_minutes', 'rank_runtime_within_year')
    return rank_by_runtime_df

# return number of null values for each column (it was used for cleaning)
def null_values_count(df):
    cols = df.columns
    null_values_count_df = df.select([f.count(f.when(f.col(c).isNull(), c))
                                       .alias(c) for c in cols])
    return null_values_count_df