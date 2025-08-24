import pyspark.sql.functions as f

def change_column_names(name_basics_df):
    name_basics_df = (name_basics_df.withColumnRenamed('primaryName', 'primary_name')
                                    .withColumnRenamed('birthYear', 'birth_year')
                                    .withColumnRenamed('deathYear', 'death_year')
                                    .withColumnRenamed('primaryProfession', 'primary_profession')
                                    .withColumnRenamed('knownForTitles', 'known_for_titles'))
    return name_basics_df

def change_n_to_none(name_basics_df):
    name_basics_df = name_basics_df.withColumn('death_year',
                                               f.when(f.col('death_year').isin("\\N"), None).otherwise(f.col('death_year')))
    return name_basics_df

def change_primary_profession_and_known_for_titles_types(name_basics_df):
    name_basics_df = name_basics_df.withColumn('primary_profession',
                                               f.split(f.col('primary_profession'), ","))
    name_basics_df = name_basics_df.withColumn('known_for_titles',
                                               f.split(f.col('known_for_titles'), ","))
    return name_basics_df

def name_basics_preprocess(name_basics_df):
    name_basics_df = change_column_names(name_basics_df)
    name_basics_df = change_n_to_none(name_basics_df)
    name_basics_df = change_primary_profession_and_known_for_titles_types(name_basics_df)
    return name_basics_df