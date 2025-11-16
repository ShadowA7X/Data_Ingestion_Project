#!/usr/bin/env python3
"""
IO utilities for reading/writing the random_users.csv file.
"""

from pathlib import Path
from typing import Tuple
import pandas as pd

def get_data_dir(BASE_DIR: Path) -> Path:
    # This function creates and returns the path to the "data" directory inside the given base directory.
    DATA_DIR = BASE_DIR / "data"
    DATA_DIR.mkdir(exist_ok=True)
    return DATA_DIR

def upsert_random_users_csv( new_rows: pd.DataFrame, BASE_DIR: Path,) -> Tuple[pd.DataFrame, Path]:
    # This function appends new rows to the random_users.csv file, removes duplicates based on the unique user ID (login.uuid),
    # and writes the updated DataFrame back to disk. It returns the final DataFrame and the path to the CSV file.

    DATA_DIR = get_data_dir(BASE_DIR)
    csv_path = DATA_DIR / "random_users.csv" # Define the full path to the random_users.csv file inside it.

    if csv_path.exists(): # If the CSV file already exists, read existing data and append new rows.
        df_existing = pd.read_csv(csv_path)
        df_final = pd.concat([df_existing, new_rows], ignore_index=True)
    else:
        df_final = new_rows
    
    # ignore_index=True means: "After combining these two tables, reset the row numbers to be a simple sequence from 0 to N-1."
    # Without ignore_index=True (The Messy Way): Pandas keeps the original page numbers. Your new, combined table would have
    # a messy index: 0, 1, ... 99 (from the old data) ...and then... 0, 1, ... 9 (from the new data)

    # Removes duplicate users based on their unique id (login.uuid), keeping the first occurrence.
    df_final = df_final.drop_duplicates(subset=["login.uuid"])

    # Writes the final table back to random_users.csv. index=False avoids writing a numeric index column.
    df_final.to_csv(csv_path, index=False)

    return df_final, csv_path
