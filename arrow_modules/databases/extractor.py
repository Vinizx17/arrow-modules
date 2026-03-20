# extractor.py
"""Data extraction accelerator for relational databases into PyArrow Tables.

This module provides a function to extract data from PostgreSQL, MySQL,
or SQL Server databases in chunks, converting them into an in-memory
PyArrow Table for downstream processing.
"""

from typing import Optional, Generator

import pyarrow as pa
import pandas as pd
from sqlalchemy.engine.base import Engine


def extract_table_to_arrow(
    engine: Engine,
    schema: str,
    table: str,
    chunk_size: int = 10000,
    columns: Optional[list[str]] = None
) -> pa.Table:
    """Extract a database table into an in-memory PyArrow Table in chunks.

    Args:
        engine (Engine): SQLAlchemy engine connected to the database.
        schema (str): Database schema name.
        table (str): Table name to extract.
        chunk_size (int, optional): Number of rows per chunk to fetch from
            the database. Defaults to 10_000.
        columns (list[str], optional): List of columns to extract. Defaults to None
            (all columns).

    Returns:
        pa.Table: PyArrow Table containing all the data from the table.
    """
    # Build SQL query
    cols = ", ".join(columns) if columns else "*"
    query = f"SELECT {cols} FROM {schema}.{table}"

    # Initialize an empty PyArrow Table
    arrow_table: Optional[pa.Table] = None

    # Read in chunks
    for chunk_df in pd.read_sql(query, engine, chunksize=chunk_size):
        chunk_arrow = pa.Table.from_pandas(chunk_df)
        if arrow_table is None:
            arrow_table = chunk_arrow
        else:
            arrow_table = pa.concat_tables([arrow_table, chunk_arrow])

    return arrow_table