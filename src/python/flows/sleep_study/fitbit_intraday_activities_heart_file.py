import pandas as pd
from athena import athena_ops as ao
from utils.json import flatten_utils as fu

MAIN_TABLE_NAME = "fitbit_intraday_activities_heart_file"
DATABASE = "mydatahelps-sleepstudy"
DESTINATION_DATABASE = "test_schema_database"

date = "2022-08-14T00:00:00+00:00"
query = "SELECT * FROM {} where _provenance['exportenddate'] >= '{}' limit 100".format(
    MAIN_TABLE_NAME, date
)


def pre_process(df):
    ## Flattening a map column
    df["_provenance"] = fu.tuple_to_dict(df["_provenance"])
    df = fu.flatten_column(df, "_provenance")
    ## Flattening a json string column
    df["content"] = fu.string_to_dict(df["content"])
    df = fu.flatten_column(df, "content")
    df = df.drop(
        [
            "filename",
            "content__activities-heart-intraday__datasetInterval",
            "content__activities-heart-intraday__datasetType",
        ],
        axis=1,
    )
    ## Extract first value of array column
    col_name = "content__activities-heart"
    df[col_name] = df[col_name].apply(lambda x: x[0])
    ## Flattening a column
    df = fu.flatten_column(df, col_name)
    return df


def process_data_for_activities_heart_table(df):
    # Exluding extra columns
    df = df.drop(["content__activities-heart-intraday__dataset"], axis=1)
    # exploding array column and flattening
    col_name = "content__activities-heart__value__heartRateZones"
    df = fu.explode_array_df_col(df, col_name)
    df = fu.flatten_column(df, col_name)
    # casting empty array to string (Type Unknown) / # exploding array column and flattening
    col_name = "content__activities-heart__value__customHeartRateZones"
    df[col_name] = df[col_name].astype(str)
    return df


def process_data_for_activities_heart_intraday_table(df):
    # Removing  recoreds where there is no dataset values
    df = df[df["content__activities-heart-intraday__dataset"].str.len() != 0]
    # selecing  columns
    df = df[
        [
            "participantidentifier",
            "content__activities-heart__dateTime",
            "content__activities-heart-intraday__dataset",
        ]
    ]
    # exploding array column and flattening
    col_name = "content__activities-heart-intraday__dataset"
    df = fu.explode_array_df_col(df, col_name)
    df = fu.flatten_column(df, col_name)
    df["_partition_day"] = df["content__activities-heart__dateTime"]
    df["content__activities-heart__dateTime"] = pd.to_datetime(
        df["content__activities-heart__dateTime"]
        + df["content__activities-heart-intraday__dataset__time"],
        format="%Y-%m-%d%H:%M:%S",
    )
    # dropping extra column
    df = df.drop(["content__activities-heart-intraday__dataset__time"], axis=1)
    return df


if __name__ == "__main__":
    df = ao.read_from_athena_via_query(query, DATABASE)
    df = pre_process(df)
    activities_heart_df = process_data_for_activities_heart_table(df)
    activities_heart_intraday_df = process_data_for_activities_heart_intraday_table(df)
    # creating tables
    configs = [
        {
            "table_name": "test_flattened_fitbit_activities_heart",
            "df": activities_heart_df,
            "partition_columns": ["content__activities-heart__dateTime"],
            "s3_path": "s3://aws-athena-query-results-411877231383-us-east-1/test_flattened_fitbit_activities_heart/",
            "dtype": {"content__activities-heart__dateTime": "date"},
            "database": DESTINATION_DATABASE,
        },
        {
            "table_name": "test_flattened_fitbit_activities_heart_intraday",
            "df": activities_heart_intraday_df,
            "partition_columns": ["_partition_day"],
            "s3_path": "s3://aws-athena-query-results-411877231383-us-east-1/test_flattened_fitbit_activities_heart_intraday/",
            "dtype": {"_partition_day": "date"},
            "database": DESTINATION_DATABASE,
        },
    ]
    for config in configs:
        ao.write_to_athena(config)
