def delete_end_year(title_basics_df):
    title_basics_df = title_basics_df.drop("end_year")
    return title_basics_df


def cleaning_title_basics_df(title_basics_df):
    title_basics_df = delete_end_year(title_basics_df)
    return title_basics_df
