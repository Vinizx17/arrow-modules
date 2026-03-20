# Storage Connector

Connects to cloud storage services: AWS S3, Azure Blob, and GCS.

```python
from arrow_modules.storage.connector import (
    connect_s3,
    connect_azure_blob,
    connect_gcs
)

# AWS S3
s3_client = connect_s3(
    access_key="AWS_KEY",
    secret_key="AWS_SECRET",
    region_name="us-east-1",
)

# Azure Blob
azure_client = connect_azure_blob(
    connection_string="DefaultEndpointsProtocol=...;AccountName=...;AccountKey=...;"
)

# Google Cloud Storage
gcs_client = connect_gcs(
    service_account_json="/path/to/service_account.json",
)