from pyspark.sql import functions as f

def delete_episode_number(title_episode_df):
    title_episode_df = title_episode_df.drop("episode_number")
    return title_episode_df


#there are rows in dataframe that looks like year (2003, 1990 etc.)
def delete_error_values_from_season_number(title_episode_df):
    title_episode_df = title_episode_df.withColumn(
        "season_number",
        f.when(f.col("season_number") > 1000, None).otherwise(f.col("season_number")))
    return title_episode_df


def cleaning_title_episode(title_episode_df):
    title_episode_df = delete_episode_number(title_episode_df)
    title_episode_df = delete_error_values_from_season_number(title_episode_df)
    return title_episode_df
