from setting import AKAS_DF_PATH, AKAS_DF_RESULTS_PATH, TITLE_CREW_DF_PATH, TITLE_CREW_DF_RESULTS_PATH, \
    TITLE_RATINGS_DF_RESULTS_PATH, TITLE_RATINGS_DF_PATH
from setting import (TITLE_PRINCIPALS_DF_PATH, TITLE_PRINCIPALS_RESULTS_PATH,
                     NAME_BASICS_DF_PATH, NAME_BASICS_DF_RESULTS_PATH)
from setting import (TITLE_BASICS_DF_PATH, TITLE_BASICS_DF_CSV_PATH,
                     TITLE_EPISODE_DF_PATH, TITLE_EPISODE_DF_CSV_PATH,
                     QUESTION_8_PATH, QUESTION_35_PATH, QUESTION_36_PATH, QUESTION_37_PATH, QUESTION_38_PATH,
                     HIGHEST_RATING_PER_YEAR, AVG_RATING_PER_GENRE)
from setting import (TITLE_RATING_WITH_VOTES_ABOVE_10000_PATH, RAQUEL_WELCH_PRIMARY_PROFESSION_PATH,
                     TITLES_WITH_GERMAN_OR_SWISS_REGION_PATH, COUNT_MOVIES_PER_YEAR_PATH,
                     THREE_POPULAR_PROFESSIONS_PATH, TOP_DIRECTOR_BY_FILM_COUNT_PATH,
                     RUNTIME_DIFF_WITHIN_TITLE_TYPE_PATH, RANK_BY_RUNTIME_WITHIN_START_YEAR_PATH,
                     TOP_FIVE_MOVIES_START_YEARS_PATH, TV_SERIES_PER_DIRECTOR_COUNT_PATH, BEST_FILM_ORIGINAL_NAME_PATH,
                     RELATION_EPISODE_AMOUNT_AND_RATING_PATH)
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
from questions import questions_mishchenia
from questions import questions_sokolova
from questions import questions_stoliaruk

import pyspark.sql.functions as f

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

# filtering questions by Mishchenia
titles_with_rating_above_5=questions_mishchenia.titles_with_rating_above_5(title_ratings_df)
titles_with_ukrainian_translation=questions_mishchenia.titles_with_ukrainian_translation(akas_df)
william_dickson_films=questions_mishchenia.william_dickson_films(title_crew_df)

# filtering questions by Sokolova
title_rating_with_votes_above_10000 = (questions_sokolova
                                       .title_rating_with_votes_above_10000(title_ratings_df))
write_df_to_csv(title_rating_with_votes_above_10000, TITLE_RATING_WITH_VOTES_ABOVE_10000_PATH)

raquel_welch_primary_profession = (questions_sokolova.raquel_welch_primary_profession(name_basics_df))
write_df_to_csv(raquel_welch_primary_profession, RAQUEL_WELCH_PRIMARY_PROFESSION_PATH)

titles_with_german_or_swiss_region = questions_sokolova.titles_with_german_or_swiss_region(akas_df)
write_df_to_csv(titles_with_german_or_swiss_region, TITLES_WITH_GERMAN_OR_SWISS_REGION_PATH)

# filtering questions by Stoliaruk
episodes_of_twin_peaks_1990 = questions_stoliaruk.episodes_of_twin_peaks_1990(title_episode_df)
more_than_2_hours_long_films = questions_stoliaruk.more_than_two_hours_long_films(title_basics_df)
children_friendly_films = questions_stoliaruk.children_friendly_films(title_basics_df)

# grouping and window functions by Mishchenia
actors_with_the_biggest_count_films = (
    questions_mishchenia.actors_with_the_biggest_count_films(title_principals_df, 5))
genre_with_the_biggest_avg_film_time = (
    questions_mishchenia.genre_with_the_biggest_avg_film_time(title_basics_df, 5))
region_with_the_biggest_translations = questions_mishchenia.region_with_the_biggest_translations(akas_df)

top_highly_rated_movies_by_votes = questions_mishchenia.top_highly_rated_movies_by_votes(title_ratings_df)
top_5_longest_movies_by_genre = questions_mishchenia.top_5_longest_movies_by_genre(title_basics_df)

# grouping and window functions by Sokolova
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

# grouping and window functions by Stoliaruk
most_released_years = questions_stoliaruk.most_released_years(title_basics_df)
write_df_to_csv(most_released_years, QUESTION_8_PATH)

genre_count_per_year = questions_stoliaruk.genre_count_per_year(title_basics_df)
write_df_to_csv(genre_count_per_year, QUESTION_35_PATH)

rating_count = questions_stoliaruk.rating_count(title_ratings_df)
write_df_to_csv(rating_count, QUESTION_36_PATH)

average_movies_runtime_per_year = questions_stoliaruk.average_movies_runtime_per_year(title_basics_df)
write_df_to_csv(average_movies_runtime_per_year, QUESTION_37_PATH)

duration_stats_per_type = questions_stoliaruk.duration_stats_per_type(title_basics_df)
write_df_to_csv(duration_stats_per_type, QUESTION_38_PATH)

# joining questions by Sokolova
top_five_movies_start_years = questions_sokolova.top_five_movies_start_years(title_basics_df, title_ratings_df)
write_df_to_csv(top_five_movies_start_years, TOP_FIVE_MOVIES_START_YEARS_PATH)

tv_series_per_director_count = questions_sokolova.tv_series_per_director_count(name_basics_df, title_basics_df, title_crew_df)
write_df_to_csv(tv_series_per_director_count, TV_SERIES_PER_DIRECTOR_COUNT_PATH)

# joining questions by Mishchenia
best_film_original_name = questions_mishchenia.best_film_original_name(title_basics_df,title_ratings_df)
write_df_to_csv(best_film_original_name, BEST_FILM_ORIGINAL_NAME_PATH)

relation_episodes_amount_and_rating=questions_mishchenia.relation_episodes_amount_and_rating(title_episode_df,title_ratings_df)
write_df_to_csv(relation_episodes_amount_and_rating, RELATION_EPISODE_AMOUNT_AND_RATING_PATH)

# joining questions by Stoliaruk
highest_rating_per_year = questions_stoliaruk.highest_rating_per_year(title_basics_df, title_ratings_df)
write_df_to_csv(highest_rating_per_year, HIGHEST_RATING_PER_YEAR)

avg_rating_per_genre = questions_stoliaruk.avg_rating_per_genre(title_basics_df, title_ratings_df)
write_df_to_csv(avg_rating_per_genre, AVG_RATING_PER_GENRE)