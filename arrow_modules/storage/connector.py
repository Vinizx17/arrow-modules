# connector.py
"""Storage connection accelerators for AWS S3, Azure Blob Storage, and GCP GCS.

This module provides reusable functions to create Python client connections
to different cloud storage providers, abstracting the connection setup.
"""

from typing import Optional

import boto3
from azure.storage.blob import BlobServiceClient
from google.cloud import storage


def connect_s3(
    access_key: str,
    secret_key: str,
    region_name: Optional[str] = None,
    endpoint_url: Optional[str] = None
) -> boto3.client:
    """Create an AWS S3 client using boto3.

    Args:
        access_key (str): AWS access key ID.
        secret_key (str): AWS secret access key.
        region_name (str, optional): AWS region (e.g., 'us-east-1'). Defaults to None.
        endpoint_url (str, optional): Custom S3 endpoint (for MinIO or local S3). Defaults to None.

    Returns:
        boto3.client: Configured S3 client.
    """
    session = boto3.session.Session()
    s3_client = session.client(
        service_name="s3",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region_name,
        endpoint_url=endpoint_url
    )
    return s3_client


def connect_azure_blob(connection_string: str) -> BlobServiceClient:
    """Create an Azure Blob Storage client.

    Args:
        connection_string (str): Azure Blob Storage connection string.

    Returns:
        BlobServiceClient: Configured Azure Blob Storage client.
    """
    client = BlobServiceClient.from_connection_string(connection_string)
    return client


def connect_gcs(service_account_json: str) -> storage.Client:
    """Create a Google Cloud Storage client.

    Args:
        service_account_json (str): Path to GCP service account JSON file.

    Returns:
        storage.Client: Configured GCS client.
    """
    client = storage.Client.from_service_account_json(service_account_json)
    return client