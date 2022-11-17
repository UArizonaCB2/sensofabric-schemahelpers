from contextlib import nullcontext

from pyarrow import fs
from pyarrow.parquet import ParquetFile


def read_parquet_schema(
    where, memory_map=False, decryption_properties=None, filesystem=None
):
    """
    Read effective Arrow schema from Parquet file metadata.
    Parameters
    ----------
    where : str (file path) or file-like object
    memory_map : bool, default False
        Create memory map when the source is a file path.
    decryption_properties : FileDecryptionProperties, default None
        Decryption properties for reading encrypted Parquet files.
    filesystem : FileSystem, default None
        If nothing passed, will be inferred based on path.
        Path will try to be found in the local on-disk filesystem otherwise
        it will be parsed as an URI to determine the filesystem.
    Returns
    -------
    schema : pyarrow.Schema
    Examples
    --------
    >>> import pyarrow as pa
    >>> import pyarrow.parquet as pq
    >>> table = pa.table({'n_legs': [4, 5, 100],
    ...                   'animal': ["Dog", "Brittle stars", "Centipede"]})
    >>> pq.write_table(table, 'example.parquet')
    >>> pq.read_schema('example.parquet')
    n_legs: int64
    animal: string
    """
    filesystem, where = fs._resolve_filesystem_and_path(where, filesystem)
    file_ctx = nullcontext()
    if filesystem is not None:
        file_ctx = where = filesystem.open_input_file(where)

    with file_ctx:
        file = ParquetFile(
            where, memory_map=memory_map, decryption_properties=decryption_properties
        )
        return file.schema.to_arrow_schema()
