import json
import logging
import sys

import awswrangler as wr
import pyarrow as pa
import pyarrow.parquet as pq
import s3fs
from models.athena_table_config import AthenaTable
from utils.parquet.parquet_utils import read_parquet_schema
from utils.s3.s3_utils import get_s3_file_content, get_s3_file_paths

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_partition_info(dir_path, fs):
    ds = pq.ParquetDataset(dir_path, filesystem=fs, validate_schema=False)
    return ds.partitions


def handle_character_case_in_schema(schema):
    fixed_schema = {}
    for field in schema:
        fixed_schema[field.lower()] = schema[field]
    return fixed_schema


def fix_schema(schema):
    fixed_schema = handle_character_case_in_schema(schema)
    for field in fixed_schema:
        # to handle map column, athena does not accept space in the map type.
        # There could be more edge cases here, difficult to predict now.
        if fixed_schema[field] == "map<string, string>":
            fixed_schema[field] = "map<string,string>"
    return fixed_schema


def get_schema(dir_path, fs):
    s3_file_paths = get_s3_file_paths(dir_path)
    schemas = []
    # To get all schemas. If parquet file will have different schemas then we need to read all the parquet file and merge schema
    # we can read just file too to get schema, if schema is not supposed to change.
    for s3_path in s3_file_paths:
        schema = read_parquet_schema(s3_path, filesystem=fs)
        schemas.append(schema)
    return pa.unify_schemas(schemas)


def create_athena_table(configs):
    fs = s3fs.S3FileSystem()
    for config in configs:
        try:
            athena_table = AthenaTable(
                config["name"],
                database=config["database"],
                path=config["path"],
                parameters=config.get("parameters"),
            )
            partitions_info = get_partition_info(athena_table.path, fs)
            schema = get_schema(athena_table.path, fs)
            schema_wr = wr._data_types.athena_types_from_pyarrow_schema(
                schema, partitions=partitions_info
            )
            fixed_schema = fix_schema(schema_wr[0])
            partition_types = schema_wr[1]
            wr.catalog.create_parquet_table(
                database=athena_table.database,
                table=athena_table.name,
                columns_types=fixed_schema,
                path=athena_table.path,
                table_type=athena_table.table_type,
                parameters=athena_table.parameters,
                partitions_types=partition_types,
            )
            logger.info(f"table created {athena_table.database}.{athena_table.name}")
        except Exception as e:
            logger.warn(
                f"table creation failed {athena_table.database}.{athena_table.name}"
            )
            logger.warn(str(e))


if __name__ == "__main__":
    config_path = sys.argv[1]
    configs = json.loads(get_s3_file_content(config_path))
    create_athena_table(configs)
