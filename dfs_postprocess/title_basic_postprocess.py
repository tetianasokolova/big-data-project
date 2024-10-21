from pyspark.sql.functions import col, when, split, trim

def change_column_names(title_basics_df):
    new_col_names = ['tconst', 'title_type', 'primary_title', 'original_title', 'is_adult', 'start_year', 'end_year', 'runtime_minutes', 'genres']
    for idx, old_col in enumerate(title_basics_df.columns):
        title_basics_df = title_basics_df.withColumnRenamed(old_col, new_col_names[idx])
    return title_basics_df


def change_n_to_none(title_basics_df):
    columns_with_n = ['end_year', 'runtime_minutes', 'genres']
    for column in columns_with_n:
        title_basics_df = title_basics_df.withColumn(column, when(col(column).isin("\\N"), None).otherwise(col(column)))
    return title_basics_df


def shift_columns(title_basics_df):
    """
    Shifts values in the specified columns of a Spark DataFrame based on the 'is_adult' column.

    This function filters title_basics_df to retain only rows where the 'is_adult' column
    does not equal 0 or 1. For the filtered rows, it performs the following operations:

    - Shifts the value of the 'is_adult' column to 'start_year'.
    - Assigns the original value of 'original_title' to the 'is_adult' column.
    - Splits the 'primary_title' column into two parts based on the pattern '\t' and
        assigns the results to 'primary_title' and 'original_title' columns after trimming whitespace.
    - Drops temporary columns created during the split.

    Finally, it combines the modified rows with the original DataFrame rows where 'is_adult' equals 0 or 1.

    Parameters:
    title_basics_df (DataFrame): The input Spark DataFrame containing movie or title information.

    Returns:
    title_basics_df (DataFrame): A new Spark DataFrame with the shifted column values.
    """
    df_filtered = title_basics_df.filter((col("is_adult") != 0) & (col("is_adult") != 1))
    df_shifted = df_filtered.withColumn("start_year", col("is_adult")) \
                            .withColumn("is_adult", col("original_title"))
    df_shifted = df_shifted.withColumn("primary_title_part1", split(df_shifted["primary_title"], r'\t"').getItem(0)) \
                       .withColumn("primary_title_part2", split(df_shifted["primary_title"], r'\t"').getItem(1))
    df_shifted = df_shifted.withColumn("primary_title", trim(col("primary_title_part1"))) \
                       .withColumn("original_title", trim(col("primary_title_part2")))
    df_shifted = df_shifted.drop("primary_title_part1", "primary_title_part2")
    title_basics_df = title_basics_df.filter((col("is_adult") == 0) | (col("is_adult") == 1)).union(df_shifted)
    return title_basics_df


def title_basics_postprocess(title_basics_df):
    title_basics_df = change_column_names(title_basics_df)
    title_basics_df = change_n_to_none(title_basics_df)
    title_basics_df = shift_columns(title_basics_df)
    return title_basics_df

