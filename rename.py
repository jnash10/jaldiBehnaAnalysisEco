import os
import glob


def rename_csv_files(directory):
    # List of prefixes we're looking for
    prefixes = [
        "aspirational_india",
        "consumption_pyramids",
        "household_income",
        "people_of_india",
    ]

    # Walk through all subdirectories
    for root, dirs, files in os.walk(directory):
        for prefix in prefixes:
            # Find all CSV files starting with the current prefix
            matching_files = glob.glob(os.path.join(root, f"{prefix}*.csv"))

            for file in matching_files:
                # Construct the new filename
                new_name = os.path.join(root, f"{prefix}.csv")

                # Rename the file
                try:
                    os.rename(file, new_name)
                    print(f"Renamed {file} to {new_name}")
                except OSError as e:
                    print(f"Error renaming {file}: {e}")


# Usage
directory_path = "."  # Replace with your directory path
rename_csv_files(directory_path)
