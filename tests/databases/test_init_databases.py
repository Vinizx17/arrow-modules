# tests/test_init.py
import pytest

def test_databases_imports():
    """Smoke test: check that database functions can be imported from the package."""
    from arrow_modules.databases import (
        connect_postgres,
        connect_mysql,
        connect_sqlserver,
        extract_table_to_arrow
    )

    # Apenas testamos se os objetos existem
    assert callable(connect_postgres)
    assert callable(connect_mysql)
    assert callable(connect_sqlserver)
    assert callable(extract_table_to_arrow)