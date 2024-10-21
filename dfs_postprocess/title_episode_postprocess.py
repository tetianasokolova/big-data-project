def change_column_names(title_episode_df):
    new_col_names = ['tconst', 'parent_tconst', 'season_number', 'episode_number']
    for idx, old_col in enumerate(title_episode_df.columns):
        title_episode_df = title_episode_df.withColumnRenamed(old_col, new_col_names[idx])
    return title_episode_df


def title_episode_postprocess(title_episode_df):
    title_episode_df = change_column_names(title_episode_df)
    return title_episode_df

