import pyspark.sql.functions as f

def change_column_names(name_basics_df):
    name_basics_df = (name_basics_df.withColumnRenamed('primaryName', 'primary_name')
                                    .withColumnRenamed('birthYear', 'birth_year')
                                    .withColumnRenamed('deathYear', 'death_year')
                                    .withColumnRenamed('primaryProfession', 'primary_profession')
                                    .withColumnRenamed('knownForTitles', 'known_for_titles'))
    return name_basics_df

def change_type_names(name_basics_df):
    pass

def name_basics_postprocess(name_basics_df):
    name_basics_df=change_column_names(name_basics_df)
    # name_basics_df=change_type_names(name_basics_df)
    return name_basics_df