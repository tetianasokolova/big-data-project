import pyspark.sql.functions as f


def change_column_names(title_akas_df):
    title_akas_df = title_akas_df.withColumnRenamed('titleId', 'title_id').withColumnRenamed('isOriginalTitle',
                                                                                             'is_original_title')
    return title_akas_df


def title_akas_postprocess(title_akas_df):
    title_akas_df = change_column_names(title_akas_df)

    return title_akas_df
