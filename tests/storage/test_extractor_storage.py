import io
import pytest
from unittest.mock import MagicMock
import pandas as pd
import pyarrow as pa
import pyarrow.orc as po
import pyarrow.parquet as pq
from arrow_modules.storage import extractor

@pytest.fixture
def csv_bytes():
    data = "col1,col2\n1,a\n2,b\n3,c"
    return data.encode()

@pytest.fixture
def parquet_bytes():
    df = pd.DataFrame({"col1": [1, 2], "col2": ["x", "y"]})
    buf = io.BytesIO()
    table = pa.Table.from_pandas(df)
    pq.write_table(table, buf)
    buf.seek(0)
    return buf.read()

@pytest.fixture
def orc_bytes():
    df = pd.DataFrame({"col1": [5, 6], "col2": ["m", "n"]})
    buf = io.BytesIO()
    table = pa.Table.from_pandas(df)
    po.write_table(table, buf)
    buf.seek(0)
    return buf.read()

@pytest.fixture
def xml_bytes():
    xml_data = b"""
    <root>
        <row><col1>1</col1><col2>a</col2></row>
        <row><col1>2</col1><col2>b</col2></row>
    </root>
    """
    return xml_data

@pytest.fixture
def json_bytes():
    data = '{"col1":1,"col2":"a"}\n{"col1":2,"col2":"b"}\n'
    return data.encode()

@pytest.fixture
def txt_bytes():
    data = "col1,col2\n1,a\n2,b"
    return data.encode()


# ------------------- _read_stream_to_arrow -------------------

@pytest.mark.parametrize("data,fmt,rows,cols", [
    ("csv_bytes", "csv", 3, 2),
    ("json_bytes", "json", 2, 2),
    ("txt_bytes", "txt", 2, 2),
])
def test_read_stream_chunks(request, data, fmt, rows, cols):
    bytes_content = request.getfixturevalue(data)
    table = extractor._read_stream_to_arrow(bytes_content, fmt, chunk_size=1)
    assert isinstance(table, pa.Table)
    assert table.num_rows == rows
    assert table.num_columns == cols

def test_read_stream_parquet(parquet_bytes):
    table = extractor._read_stream_to_arrow(parquet_bytes, "parquet")
    assert isinstance(table, pa.Table)
    assert table.num_rows == 2

def test_read_stream_orc(orc_bytes):
    table = extractor._read_stream_to_arrow(orc_bytes, "orc")
    assert isinstance(table, pa.Table)
    assert table.num_rows == 2

def test_read_stream_xml(xml_bytes):
    table = extractor._read_stream_to_arrow(xml_bytes, "xml")
    assert isinstance(table, pa.Table)
    assert table.num_rows == 2
    assert table.column_names == ["col1", "col2"]

def test_read_stream_invalid_format(csv_bytes):
    with pytest.raises(ValueError):
        extractor._read_stream_to_arrow(csv_bytes, "invalid_format")


# ------------------- extract_s3_file_to_arrow -------------------

def test_extract_s3_file_parquet(parquet_bytes):
    mock_s3 = MagicMock()
    mock_s3.get_object.return_value = {"Body": io.BytesIO(parquet_bytes)}
    table = extractor.extract_s3_file_to_arrow(mock_s3, "bucket", "file.parquet", "parquet")
    assert isinstance(table, pa.Table)
    assert table.num_rows == 2

def test_extract_s3_file_xml(xml_bytes):
    mock_s3 = MagicMock()
    mock_s3.get_object.return_value = {"Body": io.BytesIO(xml_bytes)}
    table = extractor.extract_s3_file_to_arrow(mock_s3, "bucket", "file.xml", "xml")
    assert isinstance(table, pa.Table)
    assert table.num_rows == 2


# ------------------- extract_azure_blob_file_to_arrow -------------------

def test_extract_azure_blob_parquet(parquet_bytes):
    mock_blob = MagicMock()
    mock_blob.download_blob.return_value.readall.return_value = parquet_bytes
    mock_client = MagicMock()
    mock_client.get_blob_client.return_value = mock_blob
    table = extractor.extract_azure_blob_file_to_arrow(mock_client, "container", "file.parquet", "parquet")
    assert isinstance(table, pa.Table)
    assert table.num_rows == 2

def test_extract_azure_blob_xml(xml_bytes):
    mock_blob = MagicMock()
    mock_blob.download_blob.return_value.readall.return_value = xml_bytes
    mock_client = MagicMock()
    mock_client.get_blob_client.return_value = mock_blob
    table = extractor.extract_azure_blob_file_to_arrow(mock_client, "container", "file.xml", "xml")
    assert isinstance(table, pa.Table)
    assert table.num_rows == 2


# ------------------- extract_gcs_file_to_arrow -------------------

def test_extract_gcs_file_parquet(parquet_bytes):
    mock_blob = MagicMock()
    mock_blob.download_as_bytes.return_value = parquet_bytes
    mock_bucket = MagicMock()
    mock_bucket.blob.return_value = mock_blob
    mock_client = MagicMock()
    mock_client.bucket.return_value = mock_bucket
    table = extractor.extract_gcs_file_to_arrow(mock_client, "bucket", "file.parquet", "parquet")
    assert isinstance(table, pa.Table)
    assert table.num_rows == 2

def test_extract_gcs_file_xml(xml_bytes):
    mock_blob = MagicMock()
    mock_blob.download_as_bytes.return_value = xml_bytes
    mock_bucket = MagicMock()
    mock_bucket.blob.return_value = mock_blob
    mock_client = MagicMock()
    mock_client.bucket.return_value = mock_bucket
    table = extractor.extract_gcs_file_to_arrow(mock_client, "bucket", "file.xml", "xml")
    assert isinstance(table, pa.Table)
    assert table.num_rows == 2