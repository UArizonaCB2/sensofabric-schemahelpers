import pyarrow.parquet as pq
import os
path = 'paraquet_exports/SleepStudy/fitbit_intraday_activities_heart_file/'
schemas = []
for file in os.listdir(path):
    schema = pq.read_schema(path+file)
    schemas.append(schema)
