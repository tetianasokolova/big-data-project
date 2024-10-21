from pyspark.sql.functions import col, when

def change_column_names(title_basics_df):
    new_col_names = ['tconst', 'title_type', 'primary_title', 'original_title', 'is_adult', 'start_year', 'end_year', 'runtime_minutes', 'genres']
    for idx, old_col in enumerate(title_basics_df.columns):
        title_basics_df = title_basics_df.withColumnRenamed(old_col, new_col_names[idx])
    return title_basics_df


def change_n_to_none(title_basics_df):
    columns_with_n = ['end_year', 'runtime_minutes', 'genres']
    for column in columns_with_n:
        title_basics_df = title_basics_df.withColumn(column, when(col(column).isin("\\N"), None).otherwise(col(column)))
    return title_basics_df


def title_basics_postprocess(title_basics_df):
    title_basics_df = change_column_names(title_basics_df)
    title_basics_df = change_n_to_none(title_basics_df)
    return title_basics_df

