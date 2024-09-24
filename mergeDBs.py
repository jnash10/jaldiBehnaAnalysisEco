import sqlite3
import os
from tqdm import tqdm


def combine_databases(source_dbs, target_db, chunk_size=10000):
    # Connect to the target database
    target_conn = sqlite3.connect(target_db)
    target_cursor = target_conn.cursor()

    # Outer progress bar for databases
    for source_db in tqdm(source_dbs, desc="Processing databases"):
        # Connect to the source database
        source_conn = sqlite3.connect(source_db)
        source_cursor = source_conn.cursor()

        # Get all table names from the source database
        source_cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = source_cursor.fetchall()

        # Inner progress bar for tables
        for table in tqdm(
            tables, desc=f"Tables in {os.path.basename(source_db)}", leave=False
        ):
            table_name = table[0]

            # Skip the sqlite_sequence table
            if table_name == "sqlite_sequence":
                continue

            # Fetch the CREATE TABLE sql for this table
            source_cursor.execute(
                f"SELECT sql FROM sqlite_master WHERE type='table' AND name='{table_name}';"
            )
            create_table_sql = source_cursor.fetchone()[0]

            # Create the table in the target database
            target_cursor.execute(f"DROP TABLE IF EXISTS {table_name};")
            target_cursor.execute(create_table_sql)

            # Get column names
            source_cursor.execute(f"PRAGMA table_info({table_name})")
            columns = [col[1] for col in source_cursor.fetchall()]
            placeholders = ", ".join(["?" for _ in columns])

            # Get total number of rows
            source_cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            total_rows = source_cursor.fetchone()[0]

            # Copy data in chunks with progress bar
            with tqdm(
                total=total_rows, desc=f"Copying {table_name}", unit="rows", leave=False
            ) as pbar:
                for offset in range(0, total_rows, chunk_size):
                    source_cursor.execute(
                        f"SELECT * FROM {table_name} LIMIT {chunk_size} OFFSET {offset}"
                    )
                    data = source_cursor.fetchall()

                    target_cursor.executemany(
                        f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})",
                        data,
                    )
                    target_conn.commit()

                    pbar.update(len(data))

        source_conn.close()

    target_conn.close()
    print("All databases combined successfully!")


# Usage
source_databases = [
    "aspirational_india.db",
    "consumption_pyramids.db",
    "people_of_india.db",
    "household_income.db",
]
combined_database = "ladli.db"

combine_databases(source_databases, combined_database, chunk_size=50000)
