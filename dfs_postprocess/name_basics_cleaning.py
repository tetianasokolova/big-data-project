import pyspark.sql.functions as f

def drop_birth_year_column(name_basics_df):
    return name_basics_df.drop(f.col('birth_year'))

def drop_death_year_column(name_basics_df):
    return name_basics_df.drop(f.col('death_year'))

def drop_primary_name_null_rows(name_basics_df):
    return name_basics_df.filter(f.col('primary_name').isNotNull())

def name_basics_cleaning(name_basics_df):
    name_basics_df = drop_birth_year_column(name_basics_df)
    name_basics_df = drop_death_year_column(name_basics_df)
    name_basics_df = drop_primary_name_null_rows(name_basics_df)
    return name_basics_df