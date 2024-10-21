import pyspark.sql.functions as f


def change_column_names(title_rating_df):
    title_rating_df = title_rating_df.withColumnRenamed('averageRating', 'average_rating').withColumnRenamed('numVotes',
                                                                                                             'num_votes')
    return title_rating_df


def title_rating_postprocess(title_rating_df):
    title_rating_df = change_column_names(title_rating_df)
    return title_rating_df
