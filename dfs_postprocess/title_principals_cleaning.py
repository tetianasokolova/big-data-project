import pyspark.sql.functions as f

def drop_job_column(title_principals_df):
    return title_principals_df.drop(f.col('job'))

def drop_characters_column(title_principals_df):
    return title_principals_df.drop(f.col('characters'))

def title_principals_cleaning(title_principals_df):
    title_principals_df = drop_job_column(title_principals_df)
    title_principals_df = drop_characters_column(title_principals_df)
    return title_principals_df