# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:percent
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.19.0
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# %% [markdown]
# ## Data Migration: SQL Server to Postgres

# %%
import os
import pandas as pd;
import pyodbc
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv;


# %% [markdown]
# ## 1. Load credentials
#

# %%
load_dotenv()

# %%
sql_host = os.getenv("SQL_SERVER_HOST")
sql_db = os.getenv("SQL_SERVER_DB")

# %%
print(f"SQL SERVER HOST: {sql_host}")
print(f"SQL SERVER DB: {sql_db}")

# %%
pg_host = os.getenv("POSTGRES_HOST")
pg_port = os.getenv("POSTGRES_PORT")
pg_db = os.getenv("POSTGRES_DB")
pg_user = os.getenv("POSTGRES_USER")
pg_password = os.getenv("POSTGRES_PASSWORD")

# %%
print(f"POSTGRES HOST: {pg_host}")
print(f"POSTGRES PORT: {pg_port}")
print(f"POSTGRES DB: {pg_db}")
print(f"POSTGRES USER: {pg_user}")
print(f"POSTGRES USER: {pg_password}")

# %% [markdown]
# ## Connect to SQL Server

# %% [markdown]
#

# %%
print("Connecting to SQL Server")
print(f"  Server: {sql_host}")
print(f"  Database: {sql_db}")

# %%
try:
    sql_conn_string = (
        f"Driver={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={sql_host};"
        f"DATABASE={sql_db};"
        f"Trusted_connection=yes;"
        f"Encrypt=no;"
    )

    sql_conn = pyodbc.connect(sql_conn_string)
    sql_cursor = sql_conn.cursor()
    print("[SUCCESS] -> Connection to SQL Server now live! ")
except Exception as e:
    print(f"SQL Server connection failed: {e}")
    print(""" How to troubleshoot
          > 1. Check server name is .env file correct
          > 2. Verify SQL Server is running
          > 3. Check Windows Authentication is enabled
          > 4. If certified is the problem, use Encrypt=no or TrustServerCertificate=yes
 """)

# %% [markdown]
# ## 3. Connect to PostgreSQL

# %%
print("Connecting to PostgreSQL...")
print(f"  Server: {pg_host}")
print(f"  Database: {pg_db}")

# %%
try:
    pg_conn= psycopg2.connect(
        host=pg_host,
        port=pg_port,
        database=pg_db,
        user=pg_user,
        password=pg_password
    )

    pg_cursor = pg_conn.cursor()
    pg_cursor.execute("SELECT version();")

    pg_version = pg_cursor.fetchone()[0]
    print("Connected to PostgreSQL")
    print(f" version: {pg_version[:50]}...\n")
except psycopg2.OperationalError as e:
      print(f"Postgres connection failed: {e}")
      print(""" How to troubleshoot
               > 1. Check Postgres is running
               > 2. Verify username + password
               > 3. Check database exists
""")

except Exception as e:
     print("Unexpected error: {e}")

# %% [markdown]
# ## 4. Define the tables to migrate

# %% [markdown]
# ### Migration order
#
# - Categories (no dependencies)
# - Suppliers (no dependencies)
# - Customers (no dependencies)
# - Products (dependencies on Categories and suppliers)

# %%
tables_to_migrate = ['Categories', 'Suppliers', 'Customers', 'Products']
print(tables_to_migrate)

# %%
print("Table to migrate: ")
for i, table in enumerate(tables_to_migrate,1):
    print(f"  {i}. {table}")

total_no_tbls = len(tables_to_migrate)
print(f"\nTotal  no of tables to migrate: {total_no_tbls}")

# %% [markdown]
# ### 5. Run pre-migration checks
#

# %%
print("=" * 70)
print(">>> ROW COUNTS")
print("=" * 70)

# %%
test_query = "SELECT COUNT(*) AS total_rows FROM Categories"
sql_cursor.execute(test_query)

count = sql_cursor.fetchone()[0]
print(f"Results: {count}")

# %%
baseline_counts = {}

try:
    for table in tables_to_migrate:
        row_count_query = f"SELECT COUNT(*) AS total_rows FROM {table}"

        # Warning: Do not input SQL queries with f-strings in production (this is just for the tutorial)
        # Example
        ## table = "users; DROP TABLE users; --"
        ## query = f"SELECT COUNT(*) FROM {table}"

        sql_cursor.execute(row_count_query)
        count = sql_cursor.fetchone()[0]

        baseline_counts[table]= count
        print(f"{table:15} {count:>12} rows")

    total_rows = sum(baseline_counts.values())
    print(f"{'-' * 33}")
    print(f"{'TOTAL':15} {total_rows:>12,} rows")
    print("\n Baseline captured! ")
except Exception as e:
    print(f"Failed to get baseline counts: {e}")
    raise

# %%
print("=" * 50)
print(">>> Check 2: Null COUNTS (CustomerName)")
print("=" * 50)



# %%
quality_issues = []

try:
    print("\nCHECK 2: NULL CHECKS (CustomerName)")
    sql_cursor.execute("""
        SELECT COUNT(*) AS null_count
        FROM Customers
        WHERE CustomerName IS NULL
        """)
    null_names = sql_cursor.fetchone()[0]
    if null_names > 0:
        quality_issues.append(f"  - {null_names:,} customers with Null names...")
    #print(quality_issues)


    print("\nCHECK 3: Invalid email formats")
    sql_cursor.execute(""" SELECT COUNT(*) AS invalid_email_count FROM Customers WHERE email LIKE '@Invalid' """)
    invalid_emails = sql_cursor.fetchone()[0]
    if invalid_emails > 0:
        quality_issues.append(f"  - {invalid_emails:,} emails with invalid email formats...")
    print(quality_issues)

except Exception as e:
    pass
