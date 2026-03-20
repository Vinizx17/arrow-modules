# arrow_modules/databases/__init__.py
"""Database module for Arrow-based pipelines.

This package provides:
- Database connectors for PostgreSQL, MySQL, SQL Server
- Data extraction utilities to convert tables to PyArrow Tables
"""

from .connector import connect_postgres, connect_mysql, connect_sqlserver
from .extractor import extract_table_to_arrow

__all__ = [
    "connect_postgres",
    "connect_mysql",
    "connect_sqlserver",
    "extract_table_to_arrow"
]