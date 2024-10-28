def delete_episode_number(title_episode_df):
    title_episode_df = title_episode_df.drop("episode_number")
    return title_episode_df

def cleaning_title_episode(title_episode_df):
    title_episode_df = delete_episode_number(title_episode_df)
    return title_episode_df
