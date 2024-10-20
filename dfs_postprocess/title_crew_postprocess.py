import pyspark.sql.functions as f

def change_n_to_none(title_crew_df):
    title_crew_df = title_crew_df.withColumn('writers',
                                             f.when(f.col('writers').isin("\\N"), None).otherwise(f.col('writers')))
    title_crew_df = title_crew_df.withColumn('directors',
                                             f.when(f.col('directors').isin("\\N"), None).otherwise(f.col('directors')))
    return title_crew_df


def title_crew_postprocess(title_crew_df):
    title_crew_df = change_n_to_none(title_crew_df)
    return title_crew_df
