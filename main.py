from setting import NAME_BASICS_DF_PATH
from my_io import read_name_basics_df

name_basics_df = read_name_basics_df(NAME_BASICS_DF_PATH)
name_basics_df.show()