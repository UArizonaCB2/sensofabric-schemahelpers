import json

import pandas as pd


def tuple_to_dict(df_col):
    return df_col.apply(lambda x: dict(x))


def flatten_column(df, col_name):
    prefix = "{}__".format(col_name)
    flattend_df = pd.json_normalize(df[col_name], sep="__").add_prefix(prefix)
    merged_df = merge_df_based_on_index(df, flattend_df)
    del merged_df[col_name]
    return merged_df


def merge_df_based_on_index(df1, df2):
    return df1.join(df2)


def string_to_dict(df_col):
    return df_col.apply(lambda x: json.loads(x))


def explode_array_df_col(df, col_name):
    return df.explode(col_name).reset_index(drop=True)
