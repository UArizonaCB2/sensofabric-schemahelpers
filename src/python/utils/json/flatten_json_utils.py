import os

import awswrangler as wr
import pandas as pd
from flatsplode import flatsplode

path = "paraquet_exports/SleepStudy/fitbit_intraday_activities_heart_file/"

data = pd.read_parquet(path).to_dict(orient="records")

# custom logic of flattening(may be specific for fitbit_intraday_activities_heart_file)
records = []
import json

for row in data:
    for col_name in row:
        if isinstance(row[col_name], str):
            try:
                len(json.loads(row[col_name]).keys())
                row[col_name] = json.loads(row[col_name])
            except Exception as e:
                pass
        if isinstance(row[col_name], list):
            if (
                row[col_name]
                and isinstance(row[col_name][0], tuple)
                and isinstance(row[col_name][0][0], str)
            ):
                row[col_name] = dict((x, y) for x, y in row[col_name])
    records.extend(list(flatsplode(row)))

df = pd.DataFrame(records)
df.columns = df.columns.str.replace(".", "__")
schema = wr._data_types.athena_types_from_pandas(df, 1)

DATABASE = "test_schema_database"
TABLE = "flattened_fitbit_intraday_activities_heart_file"
PATH = (
    "s3://aws-athena-query-results-411877231383-us-east-1/test_table_for_json_flatten/"
)

wr.catalog.create_parquet_table(
    database=DATABASE,
    table=TABLE,
    path=PATH,
    columns_types=schema,
)

wr.s3.to_parquet(  # Storing the data and metadata to Data Lake
    df, database=DATABASE, path=PATH, table=TABLE, dataset=True
)
