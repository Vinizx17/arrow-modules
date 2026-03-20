# Databases Extractor

This module extracts data from relational databases and returns a PyArrow Table.

```python
from arrow_modules.databases.extractor import extract_table_to_arrow

# Extract entire table
arrow_table = extract_table_to_arrow(
    engine=engine_pg,
    schema="public",
    table="customers",
)

# Extract only selected columns in chunks of 5000
arrow_table = extract_table_to_arrow(
    engine=engine_pg,
    schema="public",
    table="customers",
    columns=["id", "name", "email"],
    chunk_size=5000,
)