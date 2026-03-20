# Storage Extractor

Extracts files from cloud storage services to PyArrow Tables.

```python
from arrow_modules.storage.extractor import (
    extract_s3_file_to_arrow,
    extract_azure_blob_file_to_arrow,
    extract_gcs_file_to_arrow
)

# AWS S3
arrow_table_s3 = extract_s3_file_to_arrow(
    s3_client=s3_client,
    bucket="my-bucket",
    key="raw/test.csv",
    file_format="csv",
)

# Azure Blob
arrow_table_azure = extract_azure_blob_file_to_arrow(
    blob_client=azure_client,
    container="raw",
    blob_name="test.csv",
    file_format="csv",
)

# Google Cloud Storage
arrow_table_gcs = extract_gcs_file_to_arrow(
    gcs_client=gcs_client,
    bucket_name="my-bucket",
    blob_name="raw/test.csv",
    file_format="csv",
)