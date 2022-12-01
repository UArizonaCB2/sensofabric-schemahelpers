import awswrangler as wr
import pandas as pd
import json

main_table_name = 'fitbit_intraday_activities_heart_file'


# parquet files filter based on date max_date
input_df = wr.athena.read_sql_query(
    sql="SELECT * FROM {} ".format(main_table_name),
    database = 'mydatahelps-sleepstudy'
)


def tuple_to_dict(df_col):
    return df_col.apply(lambda x: dict(x))

def flatten_column(df,col_name):
    prefix = '{}__'.format(col_name)
    flattend_df = pd.json_normalize(df[col_name],sep = '__').add_prefix(prefix)
    merged_df = merge_df_based_on_index(df,flattend_df)
    del merged_df[col_name]
    return merged_df

def merge_df_based_on_index(df1,df2):
    return df1.join(df2)

def string_to_dict(df_col):
    return df_col.apply(lambda x: json.loads(x))

def explode_array_df_col(df,col_name):
    return df.explode(col_name).reset_index(drop=True)



df = input_df.copy()

## Flattening a map column

df['_provenance'] = tuple_to_dict(df['_provenance'])
df = flatten_column(df, '_provenance')


## Flattening a json string column
df['content'] = string_to_dict(df['content'])
df = flatten_column(df,'content')


## Extract first value of array column
col_name = 'content__activities-heart'
df[col_name]  = df[col_name].apply(lambda x:x[0])

## Flattening a  column
df = flatten_column(df,col_name)

# Exluding extra columns for table 1
new_df = df.drop(['content__activities-heart-intraday__datasetInterval',    
    'content__activities-heart-intraday__datasetType',
    'content__activities-heart-intraday__dataset'], axis=1)


# exploding array column and flattening
col_name = 'content__activities-heart__value__heartRateZones'
new_df  = explode_array_df_col(new_df,col_name)
new_df = flatten_column(new_df,col_name)

# casting empty array to string (Type Unknown) / # exploding array column and flattening
col_name  = 'content__activities-heart__value__customHeartRateZones'
new_df[col_name] =  new_df[col_name].astype(str)



# df = df[df['content__activities-heart__dateTime']=='2022-08-18']
# selecing  columns for table 2
new_df_2 = df[['participantidentifier',
        'content__activities-heart__dateTime',
        'content__activities-heart-intraday__dataset',
]]


# exploding array column and flattening
col_name = 'content__activities-heart-intraday__dataset'
new_df_2 = explode_array_df_col(new_df_2,col_name)
new_df_2 = flatten_column(new_df_2,col_name)



DATABASE = "test_schema_database"

# creating tables
configs = [
    {
        'table_name':'test_flattened_fitbit_activities_heart',
        'df':new_df,
        's3_path': "s3://aws-athena-query-results-411877231383-us-east-1/test_flattened_fitbit_activities_heart/"
    },
        {
        'table_name':'test_flattened_fitbit_activities_heart_intraday',
        'df':new_df_2,
        's3_path': "s3://aws-athena-query-results-411877231383-us-east-1/test_flattened_fitbit_activities_heart_intraday/"
    }
    ]


for config in configs:
    table_name = config['table_name']
    path = config['s3_path']
    df = config['df']
    wr.s3.to_parquet(df, database=DATABASE, path=path, table=table_name, dataset=True)



#Athena 
# 1. Fix the bug
# 2. date time join
# 3. flow check 


# Airflow Task 
    # 1. Port forward
    # 2. DNS/

# dataframe pandas errors 
# / predefined schemas


# csv s