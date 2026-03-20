from unittest.mock import patch
import pytest
from urllib.parse import unquote_plus  # <- corrigido
from arrow_modules.databases import connector


@pytest.mark.parametrize(
    "user,password,host,port,database,expected_url",
    [
        (
            "user",
            "pass",
            "localhost",
            5432,
            "db",
            "postgresql+psycopg2://user:pass@localhost:5432/db",
        ),
    ],
)
def test_connect_postgres(user, password, host, port, database, expected_url):
    """Test connect_postgres builds correct URL and returns Engine."""
    with patch("arrow_modules.databases.connector.create_engine") as mock_create_engine:
        mock_create_engine.return_value = "engine_object"
        engine = connector.connect_postgres(user, password, host, port, database)
        assert engine == "engine_object"
        mock_create_engine.assert_called_once_with(expected_url)


@pytest.mark.parametrize(
    "user,password,host,port,database,expected_url",
    [
        (
            "user",
            "pass",
            "localhost",
            3306,
            "db",
            "mysql+pymysql://user:pass@localhost:3306/db",
        ),
    ],
)
def test_connect_mysql(user, password, host, port, database, expected_url):
    """Test connect_mysql builds correct URL and returns Engine."""
    with patch("arrow_modules.databases.connector.create_engine") as mock_create_engine:
        mock_create_engine.return_value = "engine_object"
        engine = connector.connect_mysql(user, password, host, port, database)
        assert engine == "engine_object"
        mock_create_engine.assert_called_once_with(expected_url)


@pytest.mark.parametrize(
    "user,password,host,port,database,driver,expected_substring",
    [
        (
            "user",
            "pass",
            "localhost",
            1433,
            "db",
            "ODBC Driver 18 for SQL Server",
            "DRIVER=ODBC Driver 18 for SQL Server;SERVER=localhost,1433;DATABASE=db;UID=user;PWD=pass",
        ),
    ],
)
def test_connect_sqlserver(user, password, host, port, database, driver, expected_substring):
    """Test connect_sqlserver builds correct URL and returns Engine."""
    with patch("arrow_modules.databases.connector.create_engine") as mock_create_engine:
        mock_create_engine.return_value = "engine_object"
        engine = connector.connect_sqlserver(user, password, host, port, database, driver)
        assert engine == "engine_object"

        # Corrige comparação decodificando URL
        called_url = mock_create_engine.call_args[0][0]
        odbc_connect_str = unquote_plus(called_url.split("odbc_connect=")[1])  # <- aqui
        assert expected_substring in odbc_connect_str