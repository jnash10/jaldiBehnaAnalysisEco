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
            state = config.get("state", "state")  # Default to 'state' if not specified

            # Create alias for each column to avoid conflicts
            columns = [
                f"t{i}.{col} AS {table}_{col}"
                for col in cursor.execute(f"PRAGMA table_info({table})").fetchall()
            ]
            select_clauses.append(f"SELECT {', '.join(columns)} FROM {table} t{i}")

            if i > 0:
                join_clauses.append(f"""
                    JOIN {table} t{i}
                    ON t0.{table_configs[0]['hh_id']} = t{i}.{hh_id}
                    AND t0.{table_configs[0]['month']} = t{i}.{month}
                    AND t0.{table_configs[0]['year']} = t{i}.{year}
                """)

        state_condition = f"WHERE t0.{table_configs[0]['state']} IN ({','.join(['?']*len(state_values))})"

        query = f"""
        CREATE TABLE {new_table_name} AS
        {select_clauses[0]}
        {' '.join(join_clauses)}
        {state_condition}
        """

        print(f"Creating new table '{new_table_name}'...")
        print("Executing query:", query)  # Print the query for debugging
        cursor.execute(query, state_values)

        # Get the number of rows inserted
        cursor.execute(f"SELECT COUNT(*) FROM {new_table_name}")
        row_count = cursor.fetchone()[0]

        # Create the composite primary key
        primary_key_cols = [
            f"{table_configs[0]['name']}_{col}" for col in ["hh_id", "month", "year"]
        ]
        cursor.execute(f"""
            CREATE UNIQUE INDEX idx_{new_table_name}_pk 
            ON {new_table_name} ({', '.join(primary_key_cols)})
        """)

        # Commit the transaction
        conn.commit()
        print(f"Successfully created table '{new_table_name}' with {row_count} rows")
        print(
            f"Added composite primary key ({', '.join(primary_key_cols)}) to {new_table_name}"
        )

    except Exception as e:
        conn.rollback()
        print(f"An error occurred: {e}")

    finally:
        conn.close()


# Example usage
db_path = "ladli.db"
new_table_name = "filteredStates"

# Configure your tables here
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
        "state": "STATE",
    },
    {
        "name": "consumption_pyramids",
        "hh_id": "HH_ID",
        "month": "DIR_MONTH",
        "year": "DIR_YEAR",
        "state": "STATE",
    },
    {
        "name": "aspirational_india",
        "hh_id": "HH_ID",
        "month": "month",
        "year": "year",
        "state": "STATE",
    },
    # Add more table configurations as needed
]

state_values = [
    "Uttar Pradesh",
    "Madhya Pradesh",
    "Chhattisgarh",
    "Jharkhand",
    "Bihar",
]  # Add the four allowed state values here

create_joined_table(db_path, new_table_name, table_configs, state_values)
