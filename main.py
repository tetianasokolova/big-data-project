from setting import TITLE_PRINCIPALS_DF_PATH, NAME_BASICS_DF_PATH, NAME_BASICS_DF_SAVE_PATH
from my_io import read_title_principals_df, read_name_basics_df, write_name_basics_df_to_csv

title_principals_df = read_title_principals_df(TITLE_PRINCIPALS_DF_PATH)
# title_principals_df.show()

name_basics_df = read_name_basics_df(NAME_BASICS_DF_PATH)
# name_basics_df.show()