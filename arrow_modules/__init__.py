# arrow_modules/__init__.py
"""Arrow-based pipeline package for SQL and Cloud Storage.

Provides:
- Database connectors and extractors
- Cloud storage connectors and extractors
- PyArrow Table writer for multiple formats and destinations
"""

# Databases
from .databases.connector import connect_postgres, connect_mysql, connect_sqlserver
from .databases.extractor import extract_table_to_arrow

# Storage
from .storage.connector import connect_s3, connect_azure_blob, connect_gcs
from .storage.extractor import (
    extract_s3_file_to_arrow,
    extract_azure_blob_file_to_arrow,
    extract_gcs_file_to_arrow
)

# Writer
from .arrow_writer import write_arrow_table

__all__ = [
    # Databases
    "connect_postgres",
    "connect_mysql",
    "connect_sqlserver",
    "extract_table_to_arrow",
    # Storage
    "connect_s3",
    "connect_azure_blob",
    "connect_gcs",
    "extract_s3_file_to_arrow",
    "extract_azure_blob_file_to_arrow",
    "extract_gcs_file_to_arrow",
    # Writer
    "write_arrow_table"
]