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
sql_db   = os.getenv("SQL_SERVER_DB")

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
print(f"POSTGRES PASSWORD: {pg_password}")

# %% [markdown]
# ## Connect to SQL Server

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
print("=" * 60)
print(">>> ROW COUNTS")
print("=" * 60)

# %%
test_query = "SELECT COUNT(*) AS total_rows FROM Products"
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
        quality_issues.append(f" > {null_names:,} customers with Null names...")
    print(quality_issues)

    print("\nCHECK 3: Invalid email formats")
    sql_cursor.execute(""" SELECT COUNT(*) AS invalid_email_count FROM Customers WHERE email LIKE '@Invalid' """)
    invalid_emails = sql_cursor.fetchone()[0]
    if invalid_emails > 0:
        quality_issues.append(f" > {invalid_emails:,} emails with invalid email formats...")
    print(quality_issues)

    print("\nCHECK 4: NEGATIVE PRODUCTS PRICES")
    sql_cursor.execute(""" SELECT COUNT(*) As negative_product_prices_count FROM Products WHERE UnitPrice < 0 """)
    negative_price = sql_cursor.fetchone()[0]
    if negative_price > 0:
        quality_issues.append(f" > {negative_price:,} prices contain negative prices...")
    print(quality_issues)

    print("\nCHECK 4: NEGATIVE STOCK QUANTITIES")
    sql_cursor.execute("""
                        SELECT COUNT(*) As negative_stock_quantities_count
                       FROM Products
                       WHERE StockQuantity < 0
                        """)
    negative_stock_quantities = sql_cursor.fetchone()[0]
    if negative_stock_quantities > 0:
        quality_issues.append(f" > {negative_stock_quantities:,} products contain negative values...")
    print(quality_issues)

    print("\nCHECK 6: ORPHANED FOREIGN KEYS")
    sql_cursor.execute("""SELECT COUNT(*) AS orphaned_records FROM Products  prod
                        WHERE NOT EXISTS (SELECT 1 FROM Suppliers sup
                        WHERE sup.SupplierID = prod.SupplierID)
                        """)
    orphaned_fks = sql_cursor.fetchone()[0]
    if orphaned_fks > 0:
        quality_issues.append(f" > {orphaned_fks:,} products with orphaned foreign keys...")
    print(quality_issues)

    if quality_issues:
         for issue in quality_issues:
              print(issue)
    else:
         print("No data quality issues identified")

except Exception as e:
        print(f"Error ==>> Unexpected issue {e}")
        raise

# %% [markdown]
# ### 6. Get table schema

# %%
print("="*65)
print("ANALYSE TABLE SCHEMA")
print("="*65)

# %%
table_shema = {}

try:
    for table in tables_to_migrate:
        schema_query = f"""SELECT
                        COLUMN_NAME,
                        DATA_TYPE,
                        CHARACTER_MAXIMUM_LENGTH,
                        IS_NULLABLE
                        FROM INFORMATION_SCHEMA.COLUMNS
                        WHERE TABLE_NAME = ?
                        ORDER BY ORDINAL_POSITION
                        """
        schema_df = pd.read_sql(schema_query,sql_conn,params=(table,))
        #schema_df = pd.read_sql(schema_query,sql_conn)
        table_shema[table] = schema_df
        print(f"="*65)
        print(f"{table:<12}")
        print(f"\n{schema_df}")
except Exception as e:
    pass



# %% [markdown]
# ### 7. Define data type mapping

# %%
type_mapping = {
'int': 'INTEGER',
'bigint': 'BIGINT',
'smallint': 'SMALLINT',
'tinyint': 'SMALLINT',
'bit': 'BOOLEAN',
'decimal': 'NUMERIC',
'numeric': 'NUMERIC',
'money': 'NUMERIC(19,4)',
'smallmoney': 'NUMERIC(10,4)',
'float': 'DOUBLE PRECISION',
'real': 'REAL',
'datetime': 'TIMESTAMP',
'datetime2': 'TIMESTAMP',
'smalldatetime': 'TIMESTAMP',
'date': 'DATE',
'time': 'TIME',
'char': 'CHAR',
'varchar': 'VARCHAR',
'nchar': 'CHAR',
'nvarchar': 'VARCHAR',
'text': 'TEXT',
'ntext': 'TEXT'

}

# %%
print("SQL to PostgreSQL type mapping")
print()

for sql_type,pg_type in list(type_mapping.items()):
    print(f"  {sql_type:15} --->    {pg_type}")

# %%
print("="*65)
print("CREATE TABLES IN POSTGRES")
print("="*65)

# %%
try:
    for table in tables_to_migrate:
        schema = table_shema[table]
        pg_table = table.lower()

        pg_cursor.execute(f"DROP TABLE IF EXISTS {pg_table} CASCADE")
        column_definitions = []

        for idx,row in schema.iterrows():
            col_name = row['COLUMN_NAME'].lower()
            sql_type = row['DATA_TYPE']

            base_type = sql_type.lower()
            pg_type = type_mapping.get(base_type, 'TEXT')

            if idx == 0 and col_name.endswith('id')  and 'int' in sql_type.lower():
                column_definitions.append(f"{col_name} SERIAL PRIMARY KEY")
            else:
                column_definitions.append(f"{col_name} {pg_type}")

        column_string = ",\n        ".join(column_definitions)
        create_query = f"""
                        CREATE TABLE {pg_table} ({column_string})
                        """
        print(column_string)
        pg_cursor.execute(create_query)
        pg_conn.commit()
    print("\n + " + "="*55)
    print("[SUCCESS] ---> All tables created successfully!")

except psycopg2.Error as e:
    print(f"Postgres experienced an error while creating a table: {e}")
    pg_conn.rollback()
    raise

except Exception as e:
    print(f"Unexpected issue: {e}")

# %% [markdown]
# # 9. Test Migration with one table

# %%
print("="*65)
print("TESTING MIGRATION (SINGLE TABLE)")
print("="*65)

# %%
test_table = 'Customers'
pg_table = test_table.lower()

# %%
try:
    print("1. Read from SQL Server...")
    extract_query = f"SELECT * FROM {pg_table}"
    test_df = pd.read_sql(extract_query,sql_conn)

    print("2.  Transforming data types...")

    if 'IsActive' in test_df.columns:
        test_df['IsActive'] = test_df['IsActive'].astype('bool')
        print("[SUCCESS] --->> Converted IsActive: BIT ---> BOOLEAN")

        print("3. Prepare the data for loading")
        data_tuples =[tuple(row) for row in test_df.to_numpy()]
        columns = [col.lower() for col in test_df.columns]

        column_string = ', '.join(columns)
        placeholders = ', '.join(['%'] * len(columns))

        insert_query = f"""
                        INSERT INTO {pg_table} ({column_string})
                        VALUES %s
                        """
        print(f"  Prepared {len(data_tuples):,} rows")
        print("4. Insert data into PostgresSQL...")
        execute_values(pg_cursor,insert_query,data_tuples,page_size=1000)
        pg_conn.commit()

        print(f"Loaded {len(data_tuples):,} rows")

        print("5.  Verifying...")
        pg_cursor.execute(f"SELECT COUNT(*) AS total_rows FROM {pg_table}")
        pg_count =pg_cursor.fetchone()[0]

        sql_count = baseline_counts[test_table]

        if pg_count == sql_count:
            print(f"[SUCCESS] --> Verification passed: {pg_count:,} == {sql_count:,}")
        else:
            print(f"[FAILED] --> Count mismatch: {pg_count:,} != {sql_count:,}")

        print(f"\n {test_table} migration test successfully completed!")



except Exception as e:
    pg_conn.rollback()
    raise


# %% [markdown]
# # 10. Migrate remaining tables

# %%
print("="* 65)
print("MIGRATE REMAINING TABLES")
print("="* 65)

# %%
remaining_tables = [t for t in tables_to_migrate if t != 'Customers']

for table in remaining_tables:
    pg_table = table.lower()

    print(f"Migrating {table} --> {pg_table}...")

    try:
        print("1. Reading from SQL Server...")
        extract_query = f"SLEECT * FROM {table}"
        sql_df = pd.read_sql(extract_query,sql_conn)
        print(f"  Read {len(sql_df):,} rows \n\n")

        print("2.  Preparing data...")
        data_tuples = [tuple(row) for row in sql_df.to_numpy()]
        columns = [col.lower() for col in sql_df.columns]
        columns_string = ', '.join(columns)

    except Exception as e:
        pass
