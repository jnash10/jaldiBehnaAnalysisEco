import os
import sqlite3
import pandas as pd
from datetime import datetime


def create_database_connection(db_file):
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        print(f"SQLite Database connection successful. Database at {db_file}")
    except sqlite3.Error as e:
        print(f"Error connecting to database: {e}")
    return conn


def get_existing_columns(conn):
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(household_income)")
    return [row[1] for row in cursor.fetchall()]


def create_table_if_not_exists(conn, df):
    cursor = conn.cursor()

    column_definitions = []
    for col in df.columns:
        if df[col].dtype == "int64":
            column_definitions.append(f'"{col}" INTEGER')
        elif df[col].dtype == "float64":
            column_definitions.append(f'"{col}" REAL')
        else:
            column_definitions.append(f'"{col}" TEXT')

    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS household_income (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        {', '.join(column_definitions)}
    )
    """

    cursor.execute(create_table_sql)
    conn.commit()
    print("Table 'household_income' created or already exists")


def add_missing_columns(conn, df):
    existing_columns = get_existing_columns(conn)
    new_columns = [col for col in df.columns if col not in existing_columns]

    if new_columns:
        cursor = conn.cursor()
        for col in new_columns:
            if df[col].dtype == "int64":
                col_type = "INTEGER"
            elif df[col].dtype == "float64":
                col_type = "REAL"
            else:
                col_type = "TEXT"

            alter_table_sql = (
                f'ALTER TABLE household_income ADD COLUMN "{col}" {col_type}'
            )
            cursor.execute(alter_table_sql)

        conn.commit()
        print(f"Added new columns: {', '.join(new_columns)}")


def insert_data(conn, df):
    cursor = conn.cursor()

    columns = df.columns.tolist()
    placeholders = ", ".join(["?" for _ in columns])
    column_names = ", ".join([f'"{col}"' for col in columns])

    sql = f"""
    INSERT INTO household_income ({column_names})
    VALUES ({placeholders})
    """

    data = [tuple(x) for x in df.to_numpy()]

    cursor.executemany(sql, data)
    conn.commit()
    print(f"Inserted {len(df)} rows into household_income table")


def process_household_income_files(root_dir, conn):
    for dirpath, dirnames, filenames in os.walk(root_dir):
        if "household_income.csv" in filenames:
            file_path = os.path.join(dirpath, "household_income.csv")
            dir_name = os.path.basename(dirpath)

            try:
                date = datetime.strptime(dir_name, "%b %Y")
                month = date.strftime("%b")
                year = str(date.year)
            except ValueError:
                print(f"Skipping directory with invalid name format: {dir_name}")
                continue

            df = pd.read_csv(file_path)

            # Add new columns for directory-based month and year
            df["DIR_MONTH"] = month
            df["DIR_YEAR"] = year

            if "id" in df.columns:
                df = df.drop("id", axis=1)

            if (
                conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='household_income'"
                ).fetchone()
                is None
            ):
                create_table_if_not_exists(conn, df)
            else:
                add_missing_columns(conn, df)

            insert_data(conn, df)
            print(f"Processed household_income.csv for {month} {year}")


def main():
    root_dir = "raw months"  # Replace with your actual path
    db_file = "household_income.db"

    conn = create_database_connection(db_file)
    if conn is not None:
        process_household_income_files(root_dir, conn)
        conn.close()
        print(f"Database '{db_file}' has been populated with household_income data.")
    else:
        print("Failed to create database connection.")


if __name__ == "__main__":
    main()
