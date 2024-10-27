from pyspark.sql import functions as f

def delete_end_year(title_basics_df):
    title_basics_df = title_basics_df.drop("end_year")
    return title_basics_df


def fill_runtime_minutes(title_basics_df):
    """
        Fill missing values in 'runtime_minutes' of the given DataFrame.

        This function groups the rows by 'title_type' and calculates the median of 'runtime_minutes'
        for each group. It then fills any missing values in 'runtime_minutes' with the corresponding
        group median.

        Parameters:
            title_basics_df (DataFrame): The input DataFrame containing columns 'runtime_minutes'
                                          and 'title_type'. Missing values in 'runtime_minutes' will
                                          be filled based on the median of each group in
                                          'title_type'.

        Returns:
            DataFrame: A DataFrame with missing values in 'runtime_minutes' filled with the median
                       values calculated for each group defined by 'column_2'.
    """
    median_values = (title_basics_df.groupBy("title_type").
                     agg(f.expr("percentile_approx(runtime_minutes, 0.5)").alias("median")))
    filled_df = title_basics_df.join(median_values, on="title_type", how="left")
    filled_df = filled_df.withColumn(
      "runtime_minutes",
      f.when(f.col("runtime_minutes").isNull(), f.col("median")).otherwise(f.col("runtime_minutes"))
    )
    final_df = filled_df.select(title_basics_df.columns)
    return final_df


def cleaning_title_basics_df(title_basics_df):
    title_basics_df = delete_end_year(title_basics_df)
    title_basics_df = fill_runtime_minutes(title_basics_df)
    return title_basics_df
