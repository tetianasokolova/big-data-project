from setting import AKAS_DF_PATH, AKAS_DF_RESULTS_PATH, TITLE_CREW_DF_PATH, TITLE_CREW_DF_RESULTS_PATH, \
    TITLE_RATINGS_DF_RESULTS_PATH, TITLE_RATINGS_DF_PATH
from setting import (TITLE_PRINCIPALS_DF_PATH, TITLE_PRINCIPALS_RESULTS_PATH,
                     NAME_BASICS_DF_PATH, NAME_BASICS_DF_RESULTS_PATH)
from setting import (TITLE_BASICS_DF_PATH, TITLE_BASICS_DF_CSV_PATH,
                     TITLE_EPISODE_DF_PATH, TITLE_EPISODE_DF_CSV_PATH)
from setting import (COUNT_MOVIES_PER_YEAR_PATH, THREE_POPULAR_PROFESSIONS_PATH,
                     TOP_DIRECTOR_BY_FILM_COUNT_PATH)
from my_io import read_akas_df, write_akas_df_to_csv, read_title_crew_df, write_title_crew_df_to_csv, \
    read_title_ratings_df, write_title_ratings_df_to_csv
from my_io import (read_title_principals_df, write_title_principals_df_to_csv,
                   read_name_basics_df, write_name_basics_df_to_csv, write_df_to_csv)
from my_io import read_title_basics_df, write_title_basics_df_to_csv, read_title_episode_df, \
    write_title_episode_df_to_csv
from dfs_postprocess.title_akas_postprocess import title_akas_postprocess
from dfs_postprocess.title_crew_postprocess import title_crew_postprocess
from dfs_postprocess.title_raitings_postprocess import title_rating_postprocess
from dfs_postprocess.name_basics_postprocess import name_basics_postprocess
from dfs_postprocess.name_basics_cleaning import name_basics_cleaning
from dfs_postprocess.title_principals_postprocess import title_principals_postprocess
from dfs_postprocess.title_principals_cleaning import title_principals_cleaning
from dfs_postprocess.title_basics_postprocess import title_basics_postprocess
from dfs_postprocess.title_episode_postprocess import title_episode_postprocess
from dfs_postprocess.title_basics_cleaning import cleaning_title_basics_df
from dfs_postprocess.title_episode_cleaning import cleaning_title_episode
from questions import questions_sokolova

# dataframes processing
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
title_principals_df = title_principals_cleaning(title_principals_df)
write_title_principals_df_to_csv(title_principals_df, TITLE_PRINCIPALS_RESULTS_PATH)

name_basics_df = read_name_basics_df(NAME_BASICS_DF_PATH)
name_basics_df = name_basics_postprocess(name_basics_df)
name_basics_df = name_basics_cleaning(name_basics_df)
write_name_basics_df_to_csv(name_basics_df, NAME_BASICS_DF_RESULTS_PATH)

title_basics_df = read_title_basics_df(TITLE_BASICS_DF_PATH)
title_basics_df = title_basics_postprocess(title_basics_df)
title_basics_df = cleaning_title_basics_df(title_basics_df)
write_title_basics_df_to_csv(title_basics_df, TITLE_BASICS_DF_CSV_PATH)

title_episode_df = read_title_episode_df(TITLE_EPISODE_DF_PATH)
title_episode_df = title_episode_postprocess(title_episode_df)
title_episode_df = cleaning_title_episode(title_episode_df)
write_title_episode_df_to_csv(title_episode_df, TITLE_EPISODE_DF_CSV_PATH)

# aggregation, grouping and sorting questions
count_movies_per_year = questions_sokolova.count_movies_per_year(title_basics_df)
write_df_to_csv(count_movies_per_year, COUNT_MOVIES_PER_YEAR_PATH)

three_popular_professions = questions_sokolova.three_popular_professions(name_basics_df)
write_df_to_csv(three_popular_professions, THREE_POPULAR_PROFESSIONS_PATH)

top_director_by_film_count = questions_sokolova.top_director_by_film_count(title_crew_df)
write_df_to_csv(top_director_by_film_count, TOP_DIRECTOR_BY_FILM_COUNT_PATH)