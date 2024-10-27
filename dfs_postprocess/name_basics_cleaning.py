import pyspark.sql.functions as f

def drop_birth_year_column(name_basics_df):
    return name_basics_df.drop(f.col('birth_year'))

def drop_death_year_column(name_basics_df):
    return name_basics_df.drop(f.col('death_year'))