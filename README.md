# Arrow Modules

Arrow Modules (`arrow_modules`) provides **pipeline accelerators** for working with databases and cloud storage using **PyArrow**. It helps you **extract, transform, and load data** efficiently from relational databases and cloud storage (AWS S3, Azure Blob, GCS) into **PyArrow Tables** and multiple file formats.

## Features

- **Database connectors**: PostgreSQL, MySQL, SQL Server (via SQLAlchemy)
- **Database extractors**: Extract tables in chunks into PyArrow Tables
- **Cloud storage connectors**: AWS S3 (boto3), Azure Blob Storage, GCS
- **Cloud storage extractors**: Extract files (CSV, Parquet, Avro, ORC, JSON, XML, TXT) into PyArrow Tables
- **PyArrow Table writer**: Write PyArrow Tables to local or cloud storage in multiple formats (Parquet, Feather, CSV, JSON, Avro, ORC)

## Installation

```bash
pip install arrow_modules

Usage
Database Extraction
from arrow_modules import connect_postgres, extract_table_to_arrow

engine = connect_postgres(user="user", password="pass", host="localhost", port=5432, database="mydb")
table = extract_table_to_arrow(engine, schema="public", table="my_table")
Cloud Storage Extraction
from arrow_modules import connect_s3, extract_s3_file_to_arrow

s3_client = connect_s3(access_key="AKIA...", secret_key="SECRET", region_name="us-east-1")
table = extract_s3_file_to_arrow(s3_client, bucket="my-bucket", key="data/file.parquet")
Writing PyArrow Table
from arrow_modules import write_arrow_table
write_arrow_table(table, destination_path="data.parquet", file_format="parquet", storage_type="local")
Supported Formats

Parquet, Feather, CSV, JSON, Avro, ORC, TXT, XML

Supported Databases

PostgreSQL, MySQL, SQL Server

Supported Cloud Providers

AWS S3, Azure Blob Storage, Google Cloud Storage```


This project is licensed under the Apache License 2.0. See the LICENSE
