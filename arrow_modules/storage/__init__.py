# arrow_modules/storage/__init__.py
"""Storage module for Arrow-based pipelines.

This package provides:
- Cloud storage connectors (AWS S3, Azure Blob Storage, GCS)
- Data extraction utilities to convert cloud files to PyArrow Tables
"""

from .connector import connect_s3, connect_azure_blob, connect_gcs
from .extractor import (
    extract_s3_file_to_arrow,
    extract_azure_blob_file_to_arrow,
    extract_gcs_file_to_arrow
)

__all__ = [
    "connect_s3",
    "connect_azure_blob",
    "connect_gcs",
    "extract_s3_file_to_arrow",
    "extract_azure_blob_file_to_arrow",
    "extract_gcs_file_to_arrow"
]