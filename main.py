from setting import AKAS_DF_PATH, AKAS_DF_RESULTS_PATH, TITLE_CREW_DF_PATH, TITLE_CREW_DF_RESULTS_PATH, \
    TITLE_RATINGS_DF_RESULTS_PATH, TITLE_RATINGS_DF_PATH
from setting import (TITLE_PRINCIPALS_DF_PATH, TITLE_PRINCIPALS_RESULTS_PATH,
                     NAME_BASICS_DF_PATH, NAME_BASICS_DF_RESULTS_PATH)
from setting import (TITLE_BASICS_DF_PATH, TITLE_BASICS_DF_CSV_PATH,
                     TITLE_EPISODE_DF_PATH, TITLE_EPISODE_DF_CSV_PATH)
from my_io import read_akas_df, write_akas_df_to_csv, read_title_crew_df, write_title_crew_df_to_csv, \
    read_title_ratings_df, write_title_ratings_df_to_csv
from my_io import (read_title_principals_df, write_title_principals_df_to_csv,
                   read_name_basics_df, write_name_basics_df_to_csv)
from my_io import read_title_basics_df, write_title_basics_df_to_csv, read_title_episode_df, \
    write_title_episode_df_to_csv
from dfs_postprocess.title_akas_postprocess import title_akas_postprocess
from dfs_postprocess.title_crew_postprocess import title_crew_postprocess
from dfs_postprocess.title_raitings_postprocess import title_rating_postprocess
from dfs_postprocess.name_basics_postprocess import name_basics_postprocess
from dfs_postprocess.title_principals_postprocess import title_principals_postprocess
from dfs_postprocess.title_basics_postprocess import title_basics_postprocess
from dfs_postprocess.title_episode_postprocess import title_episode_postprocess
from dfs_postprocess.title_basics_cleaning import cleaning_title_basics_df
from dfs_postprocess.title_episode_cleaning import cleaning_title_episode
from questions import questions_mishchenia
from questions import questions_stoliaruk



#dataframes processing
akas_df = read_akas_df(AKAS_DF_PATH)
akas_df = title_akas_postprocess(akas_df)
write_akas_df_to_csv(akas_df, AKAS_DF_RESULTS_PATH)

title_crew_df = read_title_crew_df(TITLE_CREW_DF_PATH)
title_crew_df = title_crew_postprocess(title_crew_df)
write_title_crew_df_to_csv(title_crew_df, TITLE_CREW_DF_RESULTS_PATH)

title_ratings_df = read_title_ratings_df(TITLE_RATINGS_DF_PATH)
title_ratings_df = title_rating_postprocess(title_ratings_df)
write_title_ratings_df_to_csv(title_ratings_df, TITLE_RATINGS_DF_RESULTS_PATH)

title_principals_df = read_title_principals_df(TITLE_PRINCIPALS_DF_PATH)
title_principals_df = title_principals_postprocess(title_principals_df)
write_title_principals_df_to_csv(title_principals_df, TITLE_PRINCIPALS_RESULTS_PATH)

name_basics_df = read_name_basics_df(NAME_BASICS_DF_PATH)
name_basics_df = name_basics_postprocess(name_basics_df)
write_name_basics_df_to_csv(name_basics_df, NAME_BASICS_DF_RESULTS_PATH)

title_basics_df = read_title_basics_df(TITLE_BASICS_DF_PATH)
title_basics_df = title_basics_postprocess(title_basics_df)
title_basics_df = cleaning_title_basics_df(title_basics_df)
write_title_basics_df_to_csv(title_basics_df, TITLE_BASICS_DF_CSV_PATH)

title_episode_df = read_title_episode_df(TITLE_EPISODE_DF_PATH)
title_episode_df = title_episode_postprocess(title_episode_df)
title_episode_df = cleaning_title_episode(title_episode_df)
write_title_episode_df_to_csv(title_episode_df, TITLE_EPISODE_DF_CSV_PATH)

#questions
titles_with_rating_above_5=questions_mishchenia.titles_with_rating_above_5(title_ratings_df)
titles_with_ukrainian_translation=questions_mishchenia.titles_with_ukrainian_translation(akas_df)
william_dickson_films=questions_mishchenia.william_dickson_films(title_crew_df)

episodes_of_twin_peaks_1990 = questions_stoliaruk.episodes_of_twin_peaks_1990(title_episode_df)
more_than_2_hours_long_films = questions_stoliaruk.more_than_2_hours_long_films(title_basics_df)
children_friendly_films = questions_stoliaruk.children_friendly_films(title_basics_df)