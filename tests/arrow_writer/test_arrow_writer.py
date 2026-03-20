# tests/test_arrow_writer.py
import io
import pytest
import pyarrow as pa
from unittest.mock import Mock, patch

from arrow_modules.arrow_writer import write_arrow_table


@pytest.fixture
def sample_table():
    """Return a small PyArrow Table for testing."""
    return pa.table({"col1": [1, 2], "col2": ["a", "b"]})


def test_write_local_parquet(sample_table, tmp_path):
    """Test writing to local filesystem in Parquet format."""
    dest = tmp_path / "test.parquet"
    write_arrow_table(sample_table, str(dest), file_format="parquet", storage_type="local")
    # Check file was created
    assert dest.exists()


def test_write_s3_calls_put_object(sample_table):
    """Test writing to S3 calls put_object with correct parameters."""
    mock_s3 = Mock()
    write_arrow_table(
        sample_table,
        "myfile.parquet",
        file_format="parquet",
        storage_type="s3",
        s3_client=mock_s3,
        container_or_bucket="mybucket"
    )
    # Ensure put_object was called once
    mock_s3.put_object.assert_called_once()
    args, kwargs = mock_s3.put_object.call_args
    assert kwargs["Bucket"] == "mybucket"
    assert kwargs["Key"] == "myfile.parquet"
    # The body should be bytes
    assert isinstance(kwargs["Body"], bytes)


def test_write_azure_calls_upload_blob(sample_table):
    """Test writing to Azure Blob Storage calls upload_blob."""
    mock_blob_client = Mock()
    mock_azure = Mock()
    mock_azure.get_blob_client.return_value = mock_blob_client

    write_arrow_table(
        sample_table,
        "file.orc",
        file_format="orc",
        storage_type="azure",
        azure_blob_client=mock_azure,
        container_or_bucket="container"
    )

    mock_azure.get_blob_client.assert_called_once_with(container="container", blob="file.orc")
    mock_blob_client.upload_blob.assert_called_once()


@pytest.mark.parametrize("file_format", ["parquet", "feather", "csv", "orc"])
def test_supported_formats(sample_table, file_format, tmp_path):
    """Test that all supported formats do not raise errors for local write."""
    dest = tmp_path / f"file.{file_format}"
    write_arrow_table(sample_table, str(dest), file_format=file_format, storage_type="local")
    assert dest.exists()


def test_unsupported_format_raises(sample_table):
    """Test that using an unsupported format raises ValueError."""
    with pytest.raises(ValueError):
        write_arrow_table(sample_table, "file.xyz", file_format="xyz", storage_type="local")


def test_unsupported_storage_raises(sample_table):
    """Test that using an unsupported storage_type raises ValueError."""
    with pytest.raises(ValueError):
        write_arrow_table(sample_table, "file.parquet", storage_type="unsupported")