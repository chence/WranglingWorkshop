import os
import datetime

# 1.1
NEON_HOST = "ep-wispy-mountain-aijldqct-pooler.c-4.us-east-1.aws.neon.tech"
NEON_DB = "workshop"
NEON_USER = "neondb_owner"
NEON_PASSWORD = "npg_c74uqSvUsyPR"
NEON_PORT = 5432

CONN_INFO = dict(
    host=NEON_HOST,
    dbname=NEON_DB,
    user=NEON_USER,
    password=NEON_PASSWORD,
    port=NEON_PORT,
    sslmode="require",  # Neon typically requires SSL
)

print(CONN_INFO)

# 1.2
import psycopg2
from psycopg2.extras import execute_values

def get_conn():
    return psycopg2.connect(**CONN_INFO)

def count_tables(cur):
    tables = ["employees", "departments", "employee_departments"]
    counts = {}
    for table in tables:
        cur.execute(f"SELECT COUNT(*) FROM {table};")
        counts[table] = cur.fetchone()[0]
        print(f"Table {table} has {counts[table]} rows.")
    return counts

def clear_tables(cur):
    cur.execute("""
    TRUNCATE TABLE employee_departments, employees, departments
    RESTART IDENTITY;
    """)

if main := __name__ == "__main__":
    with get_conn() as conn:
        with conn.cursor() as cur:
            count_tables(cur)
            clear_tables(cur)
            count_tables(cur)
            # cur.execute("ALTER TABLE employees DROP CONSTRAINT employees_salary_check;")