import sqlite3
import pandas as pd
import re
from tqdm import tqdm

# Connect to the original SQLite database
original_db = "ladli.db"  # Replace with your original DB path
conn = sqlite3.connect(original_db)

# Fetch the table schema to get all column names
query = "PRAGMA table_info(filteredStates);"  # Replace 'your_table_name' with the actual table name
columns_info = pd.read_sql_query(query, conn)

# Extract the column names
columns = columns_info["name"].tolist()

# Define the patterns you want to match
patterns = [
    "M_EXP",
    "ADJ_",
    "TS_",
    "SAVING",
    "M_EXP",
    "INC",
    "TOT",
    "HEALTH",
    "CATTLE",
    "MARITAL",
    "IMC_GROUP",
    "CARS",
    "BANK",
]

# Filter the columns that match any of the patterns
matching_columns = [
    "WAVE_NO",
    "HH_ID",
    "month",
    "year",
    "STATE",
    "DISTRICT",
    "REGION_TYPE",
    "GENDER",
    "AGE_YRS",
    # "R_MEM_WGT_FOR_STATE_MS",
]

matching_columns += [
    col
    for col in columns
    if any(re.search(f"^{pattern}|{pattern}", col) for pattern in patterns)
]

# If no columns matched, raise an error
if not matching_columns:
    raise ValueError("No columns matching the patterns found!")

# print("Matched columns:")
# print(matching_columns)
print(columns)
input("Press Enter to continue...")

# Create the SQL query to select only the necessary columns
selected_columns = ", ".join(matching_columns)
select_query = f"SELECT * FROM filteredStates;"  # Replace with your table name

# Get the total number of rows for the progress bar
total_rows = pd.read_sql_query("SELECT COUNT(*) FROM filteredStates", conn).iloc[0, 0]
print(f"Total rows: {total_rows}")

# Stream data in chunks instead of loading everything at once (use chunksize for large tables)
chunk_size = 100000  # Adjust the chunk size based on memory limits
chunk_iter = pd.read_sql_query(select_query, conn, chunksize=chunk_size)

# Connect to the new SQLite database
new_db = (
    "filteredStatesAllColumns.db"  # Replace with the path to the new SQLite database
)
conn_new = sqlite3.connect(new_db)

# Write chunks to the new database incrementally with progress bar
with tqdm(total=total_rows, desc="Processing rows") as pbar:
    for chunk in chunk_iter:
        chunk.to_sql("filtered_table", conn_new, if_exists="append", index=False)
        pbar.update(len(chunk))

# Close both database connections
conn.close()
conn_new.close()

print("Data successfully extracted and written to the new database!")
