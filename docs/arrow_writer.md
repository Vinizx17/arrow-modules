# Arrow Writer

Writes PyArrow Tables to multiple formats and destinations.

```python
from arrow_modules.arrow_writer import write_arrow_table

# AWS S3
write_arrow_table(
    table=arrow_table_s3,
    destination_path="processed/test.orc",
    file_format="orc",
    storage_type="s3",
    s3_client=s3_client,
    container_or_bucket="my-bucket",
)

# Azure Blob
write_arrow_table(
    table=arrow_table_azure,
    destination_path="processed/test.orc",
    file_format="orc",
    storage_type="azure",
    azure_blob_client=azure_client,
    container_or_bucket="raw",
)

# Google Cloud Storage
write_arrow_table(
    table=arrow_table_gcs,
    destination_path="processed/test.parquet",
    file_format="parquet",
    storage_type="gcs",
    gcs_client=gcs_client,
    container_or_bucket="my-bucket",
)