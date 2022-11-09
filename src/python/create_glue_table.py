import pyarrow.parquet as pq
import os
import awswrangler as wr

path = 'paraquet_exports/SleepStudy/fitbit_intraday_activities_heart_file/'

# To get all schemas. If parquet file will have different schemas then we need to read all the parquet file and merge schema
# we can read just file too to get schema, if schema is not supposed to change.

schemas = []
for file in os.listdir(path):
    schema = pq.read_schema(path+file)
    schemas.append(schema)

#to get athena/glue schema
schema_wr = wr._data_types.athena_types_from_pyarrow_schema(schemas[0],partitions=None)


#to handle map column, athena does not accept space in the map type. 
# There could be more edge cases here, difficult to predict now. Raised the PR for this change in the awswrangler package https://github.com/aws/aws-sdk-pandas/pull/1753 
from copy import deepcopy
final_schema = deepcopy(schema_wr[0])
for field in final_schema:
    if final_schema[field] == 'map<string, string>':
        final_schema[field] = 'map<string,string>'

# create parquet table 
wr.catalog.create_parquet_table(
    database="test_schema_database",
    table="test_fitbit_intraday_activities_heart_file_with_python",
    columns_types=final_schema,
    path="s3://mydatahelps/paraquet_exports/SleepStudy/fitbit_intraday_activities_heart_file/",
    table_type="EXTERNAL_TABLE",
)
