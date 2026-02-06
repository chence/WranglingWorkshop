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

# quick connection test
with get_conn() as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT NOW();")
        print("Connected. Server time:", cur.fetchone()[0])

# 2.1 - create tables

EMPLOYEES_DDL = '''
CREATE TABLE IF NOT EXISTS employees (
    employee_id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    position TEXT NOT NULL,
    start_date DATE NOT NULL,
    salary INTEGER NOT NULL CHECK (salary BETWEEN 60000 AND 200000)
);
'''

# second table for part 3
DEPARTMENTS_DDL = '''
CREATE TABLE IF NOT EXISTS departments (
    department_id SERIAL PRIMARY KEY,
    department_name TEXT NOT NULL,
    location TEXT NOT NULL,
    annual_budget INTEGER NOT NULL CHECK (annual_budget BETWEEN 200000 AND 5000000)
);
'''

# relationship table (employee belongs to a department)
EMP_DEPT_DDL = '''
CREATE TABLE IF NOT EXISTS employee_departments (
    employee_id INTEGER PRIMARY KEY REFERENCES employees(employee_id) ON DELETE CASCADE,
    department_id INTEGER NOT NULL REFERENCES departments(department_id) ON DELETE CASCADE
);
'''

with get_conn() as conn:
    with conn.cursor() as cur:
        cur.execute(EMPLOYEES_DDL)
        cur.execute(DEPARTMENTS_DDL)
        cur.execute(EMP_DEPT_DDL)
    conn.commit()

print("Tables ensured: employees, departments, employee_departments")


from faker import Faker
import random
import pandas as pd

fake = Faker()

POSITIONS = [
    "Data Engineer",
    "Data Analyst",
    "ML Engineer",
    "Cloud Engineer",
    "DevOps Engineer",
    "Backend Developer",
    "Full Stack Developer",
    "Database Administrator",
    "Cybersecurity Analyst",
    "QA Automation Engineer",
]

DEPT_SEED = [
    ("Data Platform", "Toronto, ON"),
    ("Cloud Infrastructure", "Waterloo, ON"),
    ("Security", "Ottawa, ON"),
    ("Product Engineering", "Mississauga, ON"),
    ("Analytics", "Montreal, QC"),
]

def random_date_2015_2024():
    # Faker date_between with explicit bounds
    # return fake.date_between(start_date="2015-01-01", end_date="2024-12-31")
    return fake.date_between(
        start_date=datetime.date(2015, 1, 1),
        end_date=datetime.date(2024, 12, 31)
    )


def gen_employees(n=80, start_id=1001):
    records = []
    for i in range(n):
        emp_id = start_id + i
        name = fake.name()
        position = random.choice(POSITIONS)
        start_date = random_date_2015_2024()
        salary = random.randint(60000, 200000)
        records.append((emp_id, name, position, start_date, salary))
    return records

def gen_departments():
    rows = []
    for dept_name, loc in DEPT_SEED:
        budget = random.randint(300000, 4000000)
        rows.append((dept_name, loc, budget))
    return rows

def count_tables(cur):
    tables = ["employees", "departments", "employee_departments"]
    counts = {}
    for table in tables:
        cur.execute(f"SELECT COUNT(*) FROM {table};")
        counts[table] = cur.fetchone()[0]
        print(f"Table {table} has {counts[table]} rows.")
    return counts

with get_conn() as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM employees;")
        emp_count = cur.fetchone()[0]

        if emp_count == 0:
            print("Employees table empty → inserting data...")

            # Clear all tables first
            cur.execute("""
            TRUNCATE TABLE employee_departments, employees, departments
            RESTART IDENTITY;
            """)

            employees_rows = gen_employees(n=80, start_id=1001)  # >= 50
            departments_rows = gen_departments()

            print(len(employees_rows), len(departments_rows), employees_rows[0])

            # Insert departments
            execute_values(
                cur,
                "INSERT INTO departments (department_name, location, annual_budget) VALUES %s",
                departments_rows,
            )

            # Insert employees
            execute_values(
                cur,
                "INSERT INTO employees (employee_id, name, position, start_date, salary) VALUES %s",
                employees_rows,
            )

            # Assign each employee to a department (random mapping)
            cur.execute("SELECT department_id FROM departments;")
            dept_ids = [r[0] for r in cur.fetchall()]
            emp_dept_rows = [(emp_id, random.choice(dept_ids)) for (emp_id, *_rest) in employees_rows]

            execute_values(
                cur,
                "INSERT INTO employee_departments (employee_id, department_id) VALUES %s",
                emp_dept_rows,
            )
            conn.commit()
            print("Inserted:", len(employees_rows), "employees,", len(departments_rows), "departments, and employee_department mappings.")
        else:
            print("Skipping insertion.")
            count_tables(cur)

with get_conn() as conn:
    df_emp = pd.read_sql("SELECT * FROM employees ORDER BY employee_id;", conn)

print(df_emp.head())


print(df_emp.info())

print(df_emp.describe(include="all"))

print(df_emp.isnull().sum())

# Ensure correct dtypes
df_emp["start_date"] = pd.to_datetime(df_emp["start_date"])

# Normalize positions
df_emp["position"] = df_emp["position"].astype(str).str.strip()

# Sanity checks
salary_outside = df_emp[(df_emp["salary"] < 60000) | (df_emp["salary"] > 200000)]
date_outside = df_emp[(df_emp["start_date"] < "2015-01-01") | (df_emp["start_date"] > "2024-12-31")]

print(salary_outside.shape, date_outside.shape)

from datetime import date

today = pd.Timestamp(date.today())

df_emp["start_year"] = df_emp["start_date"].dt.year
df_emp["years_of_service"] = ((today - df_emp["start_date"]).dt.days / 365.25).round(2)

print(df_emp[["employee_id", "position", "start_date", "start_year", "years_of_service", "salary"]].head())

from sklearn.preprocessing import MinMaxScaler

scaler = MinMaxScaler()
df_scaled = df_emp.copy()

df_scaled[["salary_scaled", "years_scaled"]] = scaler.fit_transform(df_emp[["salary", "years_of_service"]])

print(df_scaled[["salary", "salary_scaled", "years_of_service", "years_scaled"]].head())

import matplotlib.pyplot as plt

# Pivot for grouped bar chart
pivot = (
    df_emp
    .groupby(["position", "start_year"])["salary"]
    .mean()
    .reset_index()
    .pivot(index="position", columns="start_year", values="salary")
    .fillna(0)
)

ax = pivot.plot(kind="bar", figsize=(14, 6))
ax.set_title("Average Salary by Position and Start Year")
ax.set_xlabel("Position")
ax.set_ylabel("Average Salary (USD)")
ax.legend(title="Start Year", bbox_to_anchor=(1.02, 1), loc="upper left")
plt.tight_layout()
plt.show()

with get_conn() as conn:
    df_joined = pd.read_sql(
        '''
        SELECT 
            e.employee_id,
            e.name,
            e.position,
            e.start_date,
            e.salary,
            d.department_name,
            d.location,
            d.annual_budget
        FROM employees e
        JOIN employee_departments ed ON e.employee_id = ed.employee_id
        JOIN departments d ON ed.department_id = d.department_id
        ORDER BY e.employee_id;
        ''',
        conn
    )

df_joined["start_date"] = pd.to_datetime(df_joined["start_date"])
df_joined["start_year"] = df_joined["start_date"].dt.year
print(df_joined.head())


import numpy as np
import matplotlib.pyplot as plt

# Create a table of avg salary by department and position
heat = (
    df_joined
    .groupby(["department_name", "position"])["salary"]
    .mean()
    .reset_index()
    .pivot(index="department_name", columns="position", values="salary")
)

# Heatmap with imshow (no seaborn)
fig, ax = plt.subplots(figsize=(14, 5))
im = ax.imshow(heat.values, aspect="auto")

ax.set_title("Heatmap: Average Salary by Department and Position")
ax.set_xlabel("Position")
ax.set_ylabel("Department")

ax.set_xticks(np.arange(heat.shape[1]))
ax.set_xticklabels(heat.columns, rotation=45, ha="right")
ax.set_yticks(np.arange(heat.shape[0]))
ax.set_yticklabels(heat.index)

# Add a colorbar
cbar = fig.colorbar(im, ax=ax)
cbar.set_label("Average Salary (USD)")

plt.tight_layout()
plt.show()

