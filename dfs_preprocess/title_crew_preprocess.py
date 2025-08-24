import pyspark.sql.functions as f


def change_n_to_none(title_crew_df):
    title_crew_df = title_crew_df.withColumn('writers',
                                             f.when(f.col('writers').isin("\\N"), None).otherwise(f.col('writers')))
    title_crew_df = title_crew_df.withColumn('directors',
                                             f.when(f.col('directors').isin("\\N"), None).otherwise(f.col('directors')))
    return title_crew_df


def split_directors_column(title_crew_df):
    title_crew_df = title_crew_df.withColumn("directors", f.split(title_crew_df["directors"], ","))
    title_crew_df = title_crew_df.withColumn("directors", f.explode(title_crew_df["directors"]))
    return title_crew_df

def split_writers_column(title_crew_df):
    title_crew_df = title_crew_df.withColumn("writers", f.split(title_crew_df["writers"], ","))
    title_crew_df = title_crew_df.withColumn("writers", f.explode(title_crew_df["writers"]))
    return title_crew_df

def drop_two_nulls_rows(title_crew_df):
    return title_crew_df.filter(~((f.col('writers').isNull()) & (f.col('directors').isNull())))


def title_crew_preprocess(title_crew_df):
    title_crew_df = split_directors_column(title_crew_df)
    title_crew_df = split_writers_column(title_crew_df)
    title_crew_df = change_n_to_none(title_crew_df)
    title_crew_df = drop_two_nulls_rows(title_crew_df)
    return title_crew_df