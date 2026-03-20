# tests/test_storage_connector.py
import pytest
from unittest.mock import patch, MagicMock

from arrow_modules.storage import connector


def test_connect_s3():
    """Test that connect_s3 returns a boto3 client."""
    with patch("boto3.session.Session.client") as mock_client:
        mock_client.return_value = "s3_client_object"
        client = connector.connect_s3(
            access_key="AKIA...",
            secret_key="SECRET",
            region_name="us-east-1",
            endpoint_url=None
        )
        assert client == "s3_client_object"
        mock_client.assert_called_once_with(
            service_name="s3",
            aws_access_key_id="AKIA...",
            aws_secret_access_key="SECRET",
            region_name="us-east-1",
            endpoint_url=None
        )


def test_connect_azure_blob():
    """Test that connect_azure_blob returns a BlobServiceClient."""
    with patch("azure.storage.blob.BlobServiceClient.from_connection_string") as mock_client:
        mock_client.return_value = "azure_client_object"
        client = connector.connect_azure_blob("DefaultEndpointsProtocol=https;AccountName=xxx;AccountKey=yyy;EndpointSuffix=core.windows.net")
        assert client == "azure_client_object"
        mock_client.assert_called_once()


def test_connect_gcs():
    """Test that connect_gcs returns a Google Cloud Storage client."""
    with patch("google.cloud.storage.Client.from_service_account_json") as mock_client:
        mock_client.return_value = "gcs_client_object"
        client = connector.connect_gcs("/path/to/service_account.json")
        assert client == "gcs_client_object"
        mock_client.assert_called_once_with("/path/to/service_account.json")