# tests/conftest.py
"""Global pytest fixtures for Arrow-based pipeline tests."""

import io
import pytest
import pandas as pd
import pyarrow as pa

from unittest.mock import MagicMock

# ------------------------
# Database fixtures
# ------------------------

@pytest.fixture
def mock_postgres_engine():
    """Mock SQLAlchemy Engine for PostgreSQL."""
    return MagicMock(name="PostgresEngine")


@pytest.fixture
def mock_mysql_engine():
    """Mock SQLAlchemy Engine for MySQL."""
    return MagicMock(name="MySQLEngine")


@pytest.fixture
def mock_sqlserver_engine():
    """Mock SQLAlchemy Engine for SQL Server."""
    return MagicMock(name="SQLServerEngine")


# ------------------------
# Storage fixtures
# ------------------------

@pytest.fixture
def mock_s3_client():
    """Mock Boto3 S3 client."""
    client = MagicMock(name="S3Client")
    obj = {"Body": io.BytesIO(b"col1,col2\n1,2\n3,4")}
    client.get_object.return_value = obj
    client.put_object.return_value = {"ResponseMetadata": {"HTTPStatusCode": 200}}
    return client


@pytest.fixture
def mock_azure_blob_client():
    """Mock Azure BlobServiceClient."""
    blob_client = MagicMock(name="BlobClient")
    container_client = MagicMock(name="ContainerClient")
    blob_client.get_blob_client.return_value = container_client
    container_client.download_blob.return_value.readall.return_value = b"col1,col2\n1,2\n3,4"
    container_client.upload_blob.return_value = None
    return blob_client


@pytest.fixture
def mock_gcs_client():
    """Mock GCS Client."""
    bucket = MagicMock(name="GCSBucket")
    blob = MagicMock(name="GCSBlob")
    blob.download_as_bytes.return_value = b"col1,col2\n1,2\n3,4"
    bucket.blob.return_value = blob

    client = MagicMock(name="GCSClient")
    client.bucket.return_value = bucket
    return client


# ------------------------
# PyArrow Table fixture
# ------------------------

@pytest.fixture
def sample_arrow_table():
    """Sample PyArrow Table for testing extraction/writing."""
    df = pd.DataFrame({"col1": [1, 3], "col2": [2, 4]})
    return pa.Table.from_pandas(df)