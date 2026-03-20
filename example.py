"""
Example main pipeline using arrow_modules abstractions.

Scenarios:
1. Extract from PostgreSQL → Parquet → AWS S3
2. Extract from SQL Server → ORC → Azure Blob
3. Extract from S3 → Avro → S3
"""

from arrow_modules.databases.connector import connect_postgres, connect_sqlserver
from arrow_modules.databases.extractor import extract_table_to_arrow
from arrow_modules.storage.connector import connect_s3, connect_azure_blob
from arrow_modules.storage.extractor import extract_s3_file_to_arrow
from arrow_modules.arrow_writer import write_arrow_table

# ---------------------------
# Scenario 1: Postgres → Parquet → S3
# ---------------------------
# Connect to Postgres
pg_engine = connect_postgres(
    user="postgres_user",
    password="postgres_pass",
    host="postgres_host",
    port=5432,
    database="my_database"
)

# Extract table to Arrow
arrow_table_pg = extract_table_to_arrow(
    engine=pg_engine,
    schema="public",
    table="sales",
    chunk_size=5000
)

# Connect to S3
s3_client = connect_s3(
    access_key="AWS_ACCESS_KEY",
    secret_key="AWS_SECRET_KEY",
    region_name="us-east-1"
)

# Write to S3 in Parquet
write_arrow_table(
    table=arrow_table_pg,
    destination_path="output/sales_pg.parquet",
    file_format="parquet",
    storage_type="s3",
    s3_client=s3_client,
    container_or_bucket="my-bucket"
)

# ---------------------------
# Scenario 2: SQL Server → ORC → Azure Blob
# ---------------------------
# Connect to SQL Server
sqlsrv_engine = connect_sqlserver(
    user="sql_user",
    password="sql_pass",
    host="sql_server_host",
    port=1433,
    database="my_database"
)

# Extract table to Arrow
arrow_table_sqlsrv = extract_table_to_arrow(
    engine=sqlsrv_engine,
    schema="dbo",
    table="customers",
    chunk_size=5000
)

# Connect to Azure Blob
azure_client = connect_azure_blob(
    connection_string="DefaultEndpointsProtocol=https;AccountName=...;AccountKey=...;EndpointSuffix=core.windows.net"
)

# Write to Azure Blob in ORC
write_arrow_table(
    table=arrow_table_sqlsrv,
    destination_path="output/customers_orc.orc",
    file_format="orc",
    storage_type="azure",
    azure_blob_client=azure_client,
    container_or_bucket="my-container"
)

# ---------------------------
# Scenario 3: S3 → Avro → S3
# ---------------------------
# Extract file from S3 to Arrow
arrow_table_s3 = extract_s3_file_to_arrow(
    s3_client=s3_client,
    bucket="my-bucket",
    key="raw_data/orders.csv",
    file_format="csv",
    chunk_size=10000
)

# Write back to S3 in Avro
write_arrow_table(
    table=arrow_table_s3,
    destination_path="processed/orders.avro",
    file_format="avro",
    storage_type="s3",
    s3_client=s3_client,
    container_or_bucket="my-bucket"
)

print("All pipelines executed successfully!")