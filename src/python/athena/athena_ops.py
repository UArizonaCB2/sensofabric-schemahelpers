import awswrangler as wr


def read_from_athena_via_query(query, database):
    return wr.athena.read_sql_query(sql=query, database=database)

def write_to_athena(config):
    table_name = config["table_name"]
    path = config["s3_path"]
    df = config["df"].copy()
    partition_columns = config["partition_columns"]
    mode = "overwrite_partitions"
    dtype = config["dtype"]
    database = config["database"]
    wr.s3.to_parquet(
        df,
        database=database,
        path=path,
        table=table_name,
        dataset=True,
        mode=mode,
        partition_cols=partition_columns,
        sanitize_columns=True,
        dtype=dtype,
    )