# tests/test_storage_init.py
import pytest

def test_storage_init_imports():
    """Test that storage package exposes all expected functions."""
    from arrow_modules.storage import (
        connect_s3,
        connect_azure_blob,
        connect_gcs,
        extract_s3_file_to_arrow,
        extract_azure_blob_file_to_arrow,
        extract_gcs_file_to_arrow
    )

    # Check that all imported objects are callable (functions)
    assert callable(connect_s3)
    assert callable(connect_azure_blob)
    assert callable(connect_gcs)
    assert callable(extract_s3_file_to_arrow)
    assert callable(extract_azure_blob_file_to_arrow)
    assert callable(extract_gcs_file_to_arrow)