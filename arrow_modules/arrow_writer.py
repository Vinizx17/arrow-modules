"""PyArrow Table writer for multiple formats and cloud storage.

This module provides a function to write PyArrow Tables to various formats
(parquet, csv, orc, feather) and destinations (local, S3, Azure Blob,
GCS) in a unified, abstracted way.
"""

import io
from typing import Optional

import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.feather as feather
import pyarrow.csv as pc
import pyarrow.orc as po

from .storage.connector import connect_s3, connect_azure_blob, connect_gcs

def write_arrow_table(
    table: pa.Table,
    destination_path: str,
    file_format: str = "parquet",
    storage_type: str = "local",
    s3_client=None,
    azure_blob_client=None,
    gcs_client=None,
    container_or_bucket: Optional[str] = None,
    chunk_size: int = 10000
) -> None:
    """Write a PyArrow Table to a chosen format and destination.

    Args:
        table (pa.Table): PyArrow Table to write.
        destination_path (str): Local path or object key in cloud storage.
        file_format (str, optional): Output format ('parquet', 'csv', 'orc', 'feather'). Defaults to 'parquet'.
        storage_type (str, optional): Storage type ('local', 's3', 'azure', 'gcs'). Defaults to 'local'.
        s3_client (optional): Boto3 S3 client, required if storage_type='s3'.
        azure_blob_client (optional): Azure BlobServiceClient, required if storage_type='azure'.
        gcs_client (optional): Google Cloud Storage client, required if storage_type='gcs'.
        container_or_bucket (str, optional): Container (Azure) or bucket (S3/GCS) name.
        chunk_size (int, optional): Rows per chunk for CSV writing. Defaults to 10000.

    Raises:
        ValueError: If storage_type or file_format is unsupported.
    """
    file_format = file_format.lower()
    storage_type = storage_type.lower()

    # Write to memory buffer
    buffer = io.BytesIO()

    if file_format == "parquet":
        pq.write_table(table, buffer)
    elif file_format == "feather":
        feather.write_feather(table, buffer)
    elif file_format == "csv":
        pc.write_csv(table, buffer)
    elif file_format == "orc":
        po.write_table(table, buffer)
    else:
        raise ValueError(
            "Unsupported file format. Supported: parquet, feather, csv, orc"
        )

    buffer.seek(0)

    # Write to storage
    if storage_type == "local":
        with open(destination_path, "wb") as f:
            f.write(buffer.read())
    elif storage_type == "s3":
        if s3_client is None or container_or_bucket is None:
            raise ValueError("s3_client and container_or_bucket must be provided for S3 storage.")
        s3_client.put_object(Bucket=container_or_bucket, Key=destination_path, Body=buffer.read())
    elif storage_type == "azure":
        if azure_blob_client is None or container_or_bucket is None:
            raise ValueError("azure_blob_client and container_or_bucket must be provided for Azure storage.")
        blob_client = azure_blob_client.get_blob_client(container=container_or_bucket, blob=destination_path)
        blob_client.upload_blob(buffer.read(), overwrite=True)
    elif storage_type == "gcs":
        if gcs_client is None or container_or_bucket is None:
            raise ValueError("gcs_client and container_or_bucket must be provided for GCS storage.")
        bucket = gcs_client.bucket(container_or_bucket)
        blob = bucket.blob(destination_path)
        blob.upload_from_file(buffer, rewind=True)
    else:
        raise ValueError("Unsupported storage_type. Supported: local, s3, azure, gcs.")