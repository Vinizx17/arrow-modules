# connector.py
"""Database connection accelerators for PostgreSQL, MySQL, and SQL Server.

This module provides reusable functions to create SQLAlchemy engine connections
to different relational databases, abstracting the connection setup.
"""
import urllib

from sqlalchemy import create_engine
from sqlalchemy.engine.base import Engine


def connect_postgres(user: str, password: str, host: str, port: int,
                     database: str) -> Engine:
    """Create a PostgreSQL SQLAlchemy engine.

    Args:
        user (str): Database username.
        password (str): Database password.
        host (str): Database host or IP address.
        port (int): Database port, typically 5432.
        database (str): Database name.

    Returns:
        Engine: SQLAlchemy engine connected to PostgreSQL.
    """
    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
    engine = create_engine(url)
    return engine


def connect_mysql(user: str, password: str, host: str, port: int,
                  database: str) -> Engine:
    """Create a MySQL SQLAlchemy engine.

    Args:
        user (str): Database username.
        password (str): Database password.
        host (str): Database host or IP address.
        port (int): Database port, typically 3306.
        database (str): Database name.

    Returns:
        Engine: SQLAlchemy engine connected to MySQL.
    """
    url = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
    engine = create_engine(url)
    return engine


def connect_sqlserver(user: str, password: str, host: str, port: int,
                      database: str,
                      driver: str = "ODBC Driver 18 for SQL Server") -> Engine:
    """Create a SQL Server SQLAlchemy engine using pyodbc.

    Args:
        user (str): Database username.
        password (str): Database password.
        host (str): Database host or IP address.
        port (int): Database port, typically 1433.
        database (str): Database name.
        driver (str, optional): ODBC driver name. Defaults to
            "ODBC Driver 18 for SQL Server".

    Returns:
        Engine: SQLAlchemy engine connected to SQL Server.
    """
    params = urllib.parse.quote_plus(
        f"DRIVER={driver};SERVER={host},{port};DATABASE={database};UID={user};PWD={password}"
    )
    url = f"mssql+pyodbc:///?odbc_connect={params}"
    engine = create_engine(url)
    return engine