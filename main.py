from setting import AKAS_DF_PATH, AKAS_DF_RESULTS_PATH, TITLE_CREW_DF_PATH, TITLE_CREW_DF_RESULTS_PATH, \
    TITLE_RATINGS_DF_RESULTS_PATH, TITLE_RATINGS_DF_PATH
from setting import (TITLE_PRINCIPALS_DF_PATH, TITLE_PRINCIPALS_RESULTS_PATH,
                     NAME_BASICS_DF_PATH, NAME_BASICS_DF_RESULTS_PATH)
from setting import (TITLE_BASICS_DF_PATH, TITLE_BASICS_DF_CSV_PATH,
                     TITLE_EPISODE_DF_PATH, TITLE_EPISODE_DF_CSV_PATH)
from setting import (TITLE_RATING_WITH_VOTES_ABOVE_10000_PATH, RAQUEL_WELCH_PRIMARY_PROFESSION_PATH,
                     TITLES_WITH_GERMAN_OR_SWISS_REGION_PATH, COUNT_MOVIES_PER_YEAR_PATH,
                     THREE_POPULAR_PROFESSIONS_PATH, TOP_DIRECTOR_BY_FILM_COUNT_PATH,
                     RUNTIME_DIFF_WITHIN_TITLE_TYPE_PATH, RANK_BY_RUNTIME_WITHIN_START_YEAR_PATH,
                     TOP_FIVE_MOVIES_START_YEARS_PATH, TV_SERIES_PER_DIRECTOR_COUNT_PATH)
from my_io import (read_akas_df, read_title_crew_df, read_title_ratings_df,
                   read_title_principals_df, read_name_basics_df, read_title_basics_df,
                   read_title_episode_df, write_df_to_csv)
from dfs_preprocess.title_akas_preprocess import title_akas_preprocess
from dfs_preprocess.title_crew_preprocess import title_crew_preprocess
from dfs_preprocess.title_raitings_preprocess import title_rating_preprocess
from dfs_preprocess.name_basics_preprocess import name_basics_preprocess
from dfs_preprocess.name_basics_cleaning import name_basics_cleaning
from dfs_preprocess.title_principals_preprocess import title_principals_preprocess
from dfs_preprocess.title_principals_cleaning import title_principals_cleaning
from dfs_preprocess.title_basics_preprocess import title_basics_preprocess
from dfs_preprocess.title_episode_preprocess import title_episode_preprocess
from dfs_preprocess.title_basics_cleaning import cleaning_title_basics_df
from dfs_preprocess.title_episode_cleaning import cleaning_title_episode
from questions import questions_sokolova

# dataframes processing
akas_df = read_akas_df(AKAS_DF_PATH)
akas_df = title_akas_preprocess(akas_df)
write_df_to_csv(akas_df, AKAS_DF_RESULTS_PATH)

title_crew_df = read_title_crew_df(TITLE_CREW_DF_PATH)
title_crew_df = title_crew_preprocess(title_crew_df)
write_df_to_csv(title_crew_df, TITLE_CREW_DF_RESULTS_PATH)

title_ratings_df = read_title_ratings_df(TITLE_RATINGS_DF_PATH)
title_ratings_df = title_rating_preprocess(title_ratings_df)
write_df_to_csv(title_ratings_df, TITLE_RATINGS_DF_RESULTS_PATH)

title_principals_df = read_title_principals_df(TITLE_PRINCIPALS_DF_PATH)
title_principals_df = title_principals_preprocess(title_principals_df)
title_principals_df = title_principals_cleaning(title_principals_df)
write_df_to_csv(title_principals_df, TITLE_PRINCIPALS_RESULTS_PATH)

name_basics_df = read_name_basics_df(NAME_BASICS_DF_PATH)
name_basics_df = name_basics_preprocess(name_basics_df)
name_basics_df = name_basics_cleaning(name_basics_df)
write_df_to_csv(name_basics_df, NAME_BASICS_DF_RESULTS_PATH)

title_basics_df = read_title_basics_df(TITLE_BASICS_DF_PATH)
title_basics_df = title_basics_preprocess(title_basics_df)
title_basics_df = cleaning_title_basics_df(title_basics_df)
write_df_to_csv(title_basics_df, TITLE_BASICS_DF_CSV_PATH)

title_episode_df = read_title_episode_df(TITLE_EPISODE_DF_PATH)
title_episode_df = title_episode_preprocess(title_episode_df)
title_episode_df = cleaning_title_episode(title_episode_df)
write_df_to_csv(title_episode_df, TITLE_EPISODE_DF_CSV_PATH)

# filtering questions by Tetiana Sokolova
title_rating_with_votes_above_10000 = (questions_sokolova
                                       .title_rating_with_votes_above_10000(title_ratings_df))
write_df_to_csv(title_rating_with_votes_above_10000, TITLE_RATING_WITH_VOTES_ABOVE_10000_PATH)

raquel_welch_primary_profession = (questions_sokolova.raquel_welch_primary_profession(name_basics_df))
write_df_to_csv(raquel_welch_primary_profession, RAQUEL_WELCH_PRIMARY_PROFESSION_PATH)

titles_with_german_or_swiss_region = questions_sokolova.titles_with_german_or_swiss_region(akas_df)
write_df_to_csv(titles_with_german_or_swiss_region, TITLES_WITH_GERMAN_OR_SWISS_REGION_PATH)

# grouping and window functions by Tetiana Sokolova
count_movies_per_year = questions_sokolova.count_movies_per_year(title_basics_df)
write_df_to_csv(count_movies_per_year, COUNT_MOVIES_PER_YEAR_PATH)

three_popular_professions = questions_sokolova.three_popular_professions(name_basics_df)
write_df_to_csv(three_popular_professions, THREE_POPULAR_PROFESSIONS_PATH)

top_director_by_film_count = questions_sokolova.top_director_by_film_count(title_crew_df, name_basics_df)
write_df_to_csv(top_director_by_film_count, TOP_DIRECTOR_BY_FILM_COUNT_PATH)

runtime_diff_within_title_type = questions_sokolova.runtime_diff_within_title_type(title_basics_df)
write_df_to_csv(runtime_diff_within_title_type, RUNTIME_DIFF_WITHIN_TITLE_TYPE_PATH)

rank_by_runtime_within_start_year = questions_sokolova.rank_by_runtime_within_start_year(title_basics_df)
write_df_to_csv(rank_by_runtime_within_start_year, RANK_BY_RUNTIME_WITHIN_START_YEAR_PATH)

# joining questions by Tetiana Sokolova
top_five_movies_start_years = questions_sokolova.top_five_movies_start_years(title_basics_df, title_ratings_df)
write_df_to_csv(top_five_movies_start_years, TOP_FIVE_MOVIES_START_YEARS_PATH)

tv_series_per_director_count = questions_sokolova.tv_series_per_director_count(name_basics_df, title_basics_df, title_crew_df)
write_df_to_csv(tv_series_per_director_count, TV_SERIES_PER_DIRECTOR_COUNT_PATH)