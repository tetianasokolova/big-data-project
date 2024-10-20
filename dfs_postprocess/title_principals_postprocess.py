def change_column_names(title_principals_df):
    # all columns are already in snake case
    return title_principals_df

def title_principals_postprocess(title_principals_df):
    title_principals_df=change_column_names(title_principals_df)
    return title_principals_df