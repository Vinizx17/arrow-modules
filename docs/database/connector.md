# Database Connector

Provides functions to create **SQLAlchemy engine connections**.

## PostgreSQL

```
from arrow_modules.databases.connector import connect_postgres

engine_pg = connect_postgres(
    user="my_user",
    password="my_password",
    host="localhost",
    port=5432,
    database="my_db"
)```

## MySQL
```
from arrow_modules.databases.connector import connect_mysql

engine_mysql = connect_mysql(
    user="my_user",
    password="my_password",
    host="localhost",
    port=3306,
    database="my_db"
)
```

## SQLServer
```
from arrow_modules.databases.connector import connect_sqlserver

engine_sqlserver = connect_sqlserver(
    user="my_user",
    password="my_password",
    host="localhost",
    port=1433,
    database="my_db"
)
```