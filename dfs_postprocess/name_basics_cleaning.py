import pyspark.sql.functions as f

def drop_birth_year_column(name_basics_df):
    return name_basics_df.drop(f.col('birth_year'))

def drop_death_year_column(name_basics_df):
    return name_basics_df.drop(f.col('death_year'))

def drop_primary_name_null_rows(name_basics_df):
    return name_basics_df.filter(f.col('primary_name').isNotNull())

def fill_null_primary_profession(name_basics_df):
    # split the primary_profession column to compute the mode
    name_basics_split_df = name_basics_df.withColumn('primary_profession',
                                               f.explode(f.split(f.col('primary_profession'), ',')))

    # select primary profession row
    profession_df = name_basics_split_df.select(f.col('primary_profession'))

    # compute the count for each profession
    profession_counts = (profession_df.groupBy('primary_profession')
                                      .agg(f.count('*').alias('count')))

    # compute the mode (the most frequent profession)
    mode_profession_df = (profession_counts.orderBy(f.col('count').desc())
                                        .select('primary_profession')
                                        .first())
    # the most frequent profession is 'actor'
    mode_profession_value = mode_profession_df['primary_profession']

    # fill missed values with mode
    name_basics_df = name_basics_df.fillna(mode_profession_value, subset=['primary_profession'])
    return name_basics_df

def fill_null_known_for_titles(name_basics_df):
    return name_basics_df.fillna('unknown_title', subset=['known_for_titles'])

def name_basics_cleaning(name_basics_df):
    name_basics_df = drop_birth_year_column(name_basics_df)
    name_basics_df = drop_death_year_column(name_basics_df)
    name_basics_df = drop_primary_name_null_rows(name_basics_df)
    name_basics_df = fill_null_primary_profession(name_basics_df)
    name_basics_df = fill_null_known_for_titles(name_basics_df)
    return name_basics_df