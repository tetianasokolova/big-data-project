import pyspark.sql.functions as f

def change_column_names(title_principals_df):
    # all columns are already in snake case
    return title_principals_df

def change_n_to_none(title_principals_df):
    columns_with_n = ['job', 'characters']
    for column in columns_with_n:
        title_principals_df = title_principals_df.withColumn(column,
                                                 f.when(f.col(column).isin("\\N"), None).otherwise(f.col(column)))
    return title_principals_df

def change_characters_column(title_principals_df):
    title_principals_df = title_principals_df.withColumn('characters',
                                                         f.regexp_replace(
                                                             f.col('characters'), r'[\[\]"]', ""))
    return title_principals_df

def title_principals_postprocess(title_principals_df):
    title_principals_df = change_column_names(title_principals_df)
    title_principals_df = change_n_to_none(title_principals_df)
    title_principals_df = change_characters_column(title_principals_df)
    return title_principals_df