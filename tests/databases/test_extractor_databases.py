# tests/test_extractor.py
"""Unit tests for extract_table_to_arrow function in extractor.py."""

from unittest.mock import patch, MagicMock
import pandas as pd
import pyarrow as pa
import pytest

from arrow_modules.databases import extractor


def test_extract_table_to_arrow_single_chunk():
    """Test extract_table_to_arrow returns PyArrow Table for a single chunk."""

    # Mock DataFrame to be returned by pd.read_sql
    df_mock = pd.DataFrame({
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"]
    })

    # Mock engine (can be anything, will not be used)
    engine_mock = MagicMock()

    with patch("arrow_modules.databases.extractor.pd.read_sql", return_value=[df_mock]):
        table = extractor.extract_table_to_arrow(engine_mock, "public", "users")

    assert isinstance(table, pa.Table)
    assert table.num_rows == 3
    assert table.num_columns == 2
    assert table.column_names == ["id", "name"]


def test_extract_table_to_arrow_multiple_chunks():
    """Test extract_table_to_arrow returns PyArrow Table for multiple chunks."""

    # Mock two chunks as DataFrames
    df_chunk1 = pd.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]})
    df_chunk2 = pd.DataFrame({"id": [3, 4], "name": ["Charlie", "David"]})

    engine_mock = MagicMock()

    # Mock pd.read_sql to yield chunks
    def mock_read_sql(query, engine, chunksize):
        yield df_chunk1
        yield df_chunk2

    with patch("arrow_modules.databases.extractor.pd.read_sql", side_effect=mock_read_sql):
        table = extractor.extract_table_to_arrow(engine_mock, "public", "users", chunk_size=2)

    assert isinstance(table, pa.Table)
    assert table.num_rows == 4
    assert table.num_columns == 2
    assert table.column_names == ["id", "name"]


def test_extract_table_to_arrow_with_columns():
    """Test extract_table_to_arrow returns PyArrow Table with selected columns."""

    # Mock DataFrame deve ter apenas as colunas solicitadas
    df_mock = pd.DataFrame({"id": [1], "age": [30]})
    engine_mock = MagicMock()

    with patch("arrow_modules.databases.extractor.pd.read_sql", return_value=[df_mock]):
        table = extractor.extract_table_to_arrow(
            engine_mock,
            "public",
            "users",
            columns=["id", "age"]
        )

    assert isinstance(table, pa.Table)
    assert table.num_rows == 1
    assert table.num_columns == 2
    assert table.column_names == ["id", "age"]