"""Full-featured storage data extraction accelerator into PyArrow Tables.

This module provides functions to extract data from AWS S3, Azure Blob Storage,
and Google Cloud Storage in multiple file formats (CSV, Parquet, ORC, XML, TXT, JSON)
in chunks, returning an in-memory PyArrow Table ready for downstream processing.
"""

import io
from typing import Optional

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.csv as pc
import pyarrow.json as pj
import pyarrow.orc as po
import xml.etree.ElementTree as ET

from .connector import connect_s3, connect_azure_blob, connect_gcs


def _read_stream_to_arrow(
    stream: bytes,
    file_format: str,
    chunk_size: int = 10000
) -> pa.Table:
    """Internal helper to convert byte stream to PyArrow Table.

    Args:
        stream (bytes): Byte content of the file.
        file_format (str): File format ('csv', 'parquet', 'orc', 'xml', 'txt', 'json').
        chunk_size (int, optional): Number of rows per chunk (for CSV, TXT, JSON). Defaults to 10_000.

    Returns:
        pa.Table: PyArrow Table containing all the data.
    """
    file_format = file_format.lower()
    if file_format == "parquet":
        return pq.read_table(io.BytesIO(stream))
    elif file_format in ("csv", "txt"):
        arrow_table: Optional[pa.Table] = None
        for chunk_df in pd.read_csv(io.BytesIO(stream), chunksize=chunk_size):
            chunk_arrow = pa.Table.from_pandas(chunk_df)
            arrow_table = chunk_arrow if arrow_table is None else pa.concat_tables([arrow_table, chunk_arrow])
        return arrow_table
    elif file_format == "json":
        arrow_table: Optional[pa.Table] = None
        for chunk_df in pd.read_json(io.BytesIO(stream), lines=True, chunksize=chunk_size):
            chunk_arrow = pa.Table.from_pandas(chunk_df)
            arrow_table = chunk_arrow if arrow_table is None else pa.concat_tables([arrow_table, chunk_arrow])
        return arrow_table
    elif file_format == "orc":
        return po.read_table(io.BytesIO(stream))
    elif file_format == "xml":
        root = ET.fromstring(stream)
        rows = []
        for child in root:
            row = {elem.tag: elem.text for elem in child}
            rows.append(row)
        df = pd.DataFrame(rows)
        return pa.Table.from_pandas(df)
    else:
        raise ValueError(
            "Unsupported file format. Supported: csv, parquet, orc, xml, txt, json"
        )


def extract_s3_file_to_arrow(
    s3_client,
    bucket: str,
    key: str,
    file_format: str = "parquet",
    chunk_size: int = 10000
) -> pa.Table:
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    stream = obj['Body'].read()
    return _read_stream_to_arrow(stream, file_format, chunk_size)


def extract_azure_blob_file_to_arrow(
    blob_client,
    container: str,
    blob_name: str,
    file_format: str = "parquet",
    chunk_size: int = 10000
) -> pa.Table:
    blob = blob_client.get_blob_client(container=container, blob=blob_name)
    stream = blob.download_blob().readall()
    return _read_stream_to_arrow(stream, file_format, chunk_size)


def extract_gcs_file_to_arrow(
    gcs_client,
    bucket_name: str,
    blob_name: str,
    file_format: str = "parquet",
    chunk_size: int = 10000
) -> pa.Table:
    bucket = gcs_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    stream = blob.download_as_bytes()
    return _read_stream_to_arrow(stream, file_format, chunk_size)