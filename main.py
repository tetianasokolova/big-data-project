from setting import (TITLE_PRINCIPALS_DF_PATH, TITLE_PRINCIPALS_RESULTS_PATH,
                     NAME_BASICS_DF_PATH, NAME_BASICS_DF_RESULTS_PATH)
from my_io import (read_title_principals_df, write_title_principals_df_to_csv,
                   read_name_basics_df, write_name_basics_df_to_csv)
from dfs_postprocess.name_basics_postprocess import name_basics_postprocess
from dfs_postprocess.title_principals_postprocess import title_principals_postprocess

title_principals_df = read_title_principals_df(TITLE_PRINCIPALS_DF_PATH)
title_principals_df = title_principals_postprocess(title_principals_df)
write_title_principals_df_to_csv(title_principals_df, TITLE_PRINCIPALS_RESULTS_PATH)
title_principals_df.show()

name_basics_df = read_name_basics_df(NAME_BASICS_DF_PATH)
name_basics_df = name_basics_postprocess(name_basics_df)
write_name_basics_df_to_csv(name_basics_df, NAME_BASICS_DF_RESULTS_PATH)
name_basics_df.show()