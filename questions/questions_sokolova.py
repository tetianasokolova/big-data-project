import pyspark.sql.functions as f

# return number of null values for each column (it was used for cleaning)
def null_values_count(df):
    cols = df.columns
    null_values_count_df = df.select([f.count(f.when(f.col(c).isNull(), c))
                                       .alias(c) for c in cols])
    return null_values_count_df