import pyspark.sql.functions as f

# question 24: які унікальні ідентифікатори та рейтинги у фільмів, що мають більше як 10,000 голосів?
def title_rating_with_votes_above_10000(title_rating_df):
    return (title_rating_df.filter(f.col('num_votes') > 10000)
                           .select(['tconst', 'average_rating']))

# return number of null values for each column (it was used for cleaning)
def null_values_count(df):
    cols = df.columns
    null_values_count_df = df.select([f.count(f.when(f.col(c).isNull(), c))
                                       .alias(c) for c in cols])
    return null_values_count_df