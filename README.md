# smart_health


To create athena table:
`make path=s3://athenatestbucket-4118/exports/config.json create_athena_table`

Content:
    s3://athenatestbucket-4118/exports/config.json
```
[
  {
    "database": "test_schema_database",
    "name": "test_all_props_partition",
    "path": "s3://athenatestbucket-4118/exports/test_partition"
  },
  {
    "database": "test_schema_database",
    "name": "test_all_props_new",
    "path": "s3://mydatahelps/paraquet_exports/SleepStudy/fitbit_intraday_activities_heart_file/",
    "parameters": {
      "has_encrypted_data": "true",
      "write.compression": "GZIP"
    }
  }
]
```

