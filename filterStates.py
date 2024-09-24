import sqlite3
from tqdm import tqdm


def create_joined_table(db_path, new_table_name, table_configs, state_values):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    try:
        # Start a transaction
        conn.execute("BEGIN TRANSACTION")

        # Construct the JOIN query
        select_clauses = []
        join_clauses = []
        for i, config in enumerate(table_configs):
            table = config["name"]
            hh_id = config["hh_id"]
            month = config["month"]
            year = config["year"]
            state = config.get("state", "STATE")  # Default to 'STATE' if not specified

            if i == 0:
                select_clauses.append(f"SELECT * FROM {table}")
                base_table = table
            else:
                join_clauses.append(f"""
                    JOIN {table}
                    ON {base_table}.{table_configs[0]['hh_id']} = {table}.{hh_id}
                    AND {base_table}.{table_configs[0]['month']} = {table}.{month}
                    AND {base_table}.{table_configs[0]['year']} = {table}.{year}
                """)

        # Use placeholders for state values
        state_placeholders = ",".join(["?" for _ in state_values])
        state_condition = (
            f"WHERE {base_table}.{table_configs[0]['state']} IN ({state_placeholders})"
        )

        query = f"""
        CREATE TABLE {new_table_name} AS
        {select_clauses[0]}
        {' '.join(join_clauses)}
        {state_condition}
        """

        print(f"Creating new table '{new_table_name}'...")
        print("Executing query:", query)  # Print the query for debugging
        print("State values:", state_values)  # Print state values for verification
        cursor.execute(query, state_values)

        # Get the number of rows inserted
        cursor.execute(f"SELECT COUNT(*) FROM {new_table_name}")
        row_count = cursor.fetchone()[0]

        # Commit the transaction
        conn.commit()
        print(f"Successfully created table '{new_table_name}' with {row_count} rows")

    except Exception as e:
        conn.rollback()
        print(f"An error occurred: {e}")

    finally:
        conn.close()


# Example usage
db_path = "ladli.db"
new_table_name = "filteredStates"

table_configs = [
    {
        "name": "people_of_india",
        "hh_id": "HH_ID",
        "month": "month",
        "year": "year",
        "state": "STATE",
    },
    {
        "name": "household_income",
        "hh_id": "HH_ID",
        "month": "DIR_MONTH",
        "year": "DIR_YEAR",
    },
    {
        "name": "consumption_pyramids",
        "hh_id": "HH_ID",
        "month": "DIR_MONTH",
        "year": "DIR_YEAR",
    },
    {
        "name": "aspirational_india",
        "hh_id": "HH_ID",
        "month": "month",
        "year": "year",
    },
]

state_values = [
    "Uttar Pradesh",
    "Madhya Pradesh",
    "Chhattisgarh",
    "Jharkhand",
    "Bihar",
]

create_joined_table(db_path, new_table_name, table_configs, state_values)
