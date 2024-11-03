import pyspark.sql.functions as f

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

# return number of null values for each column (it was used for cleaning)
def null_values_count(df):
    cols = df.columns
    null_values_count_df = df.select([f.count(f.when(f.col(c).isNull(), c))
                                       .alias(c) for c in cols])
    return null_values_count_df