#!/usr/bin/env python3 
# The line above tells your operating system (OS) which interpreter to use to run the file. It's necesary
# when you run the script directly from the command line (e.g., ./API_Ingestion.py) instead of via python API_Ingestion.py.
# Make sure the script file has execute permissions (chmod +x API_Ingestion.py).
# -----------------------------------------------------------------------------------------------------------
import os #for filesystem operations and environment variables
# Python uses the os tool to interact with the operating system. We use the os module whenever we need our
# script to read or change things at the operating system level (like change directories, environment variables, etc.).
# Use os for other OS tasks that pathlib doesn't do.
# -----------------------------------------------------------------------------------------------------------

from pathlib import Path # We use Path objects to handle file and directory paths. Use pathlib for all path-related tasks.
import requests # It helps your Python script to communicate with webs and APIs over HTTP, and get and push information from them.
import pandas as pd # It helps you to read, write, clean and manipulate tabular data like CSV files and databases.
from dotenv import load_dotenv # It loads environment variables from a .env file into your script's environment.

# --------------------------------------------------------------------------------------------------------
# Cryptography imports
# --------------------------------------------------------------------------------------------------------
import base64, hmac, hashlib # built-in modules for encoding, hashing, and cryptographic operations.
from argon2 import PasswordHasher # It provides secure password hashing using the Argon2 algorithm.
from cryptography.fernet import Fernet # It provides easy-to-use symmetric encryption and decryption for sensitive data.

#--------------------------------------------------------------------------------------------------------
# Work relative to this file (repo portability)
#--------------------------------------------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent 
# It gets the absolute path to the directory (Data_Ingstion) where this script (API_Ingestion.py) is located.
os.chdir(BASE_DIR) 
# Change the current working directory to BASE_DIR. With this you guarantee that when load_dotenv() runs, 
# it is "standing" in the correct folder and can easily find the .env file right next to it.

load_dotenv() # This read your secret .env file and load those secrets into the script's environment.

# ---- Secrets from .env
# These are secret keys used for hashing and encryption, loaded from .env file thanks to load_dotenv().
# Think on load_dotenv() as a bridge that brings your secrets from the .env file into your script's environment
# and os.environ lets you access them.

PEPPER = os.environ["PEPPER"] 
# Stores the cryptographically secure random string used to "pepper" passwords before hashing.
# We will use this to hash customer passwords securely with Argon2id.

FERNET_KEY = os.environ["FERNET_KEY"].encode() 
# Stores the base64-encoded key. The encode() converts the string in your secret into bytes beucause Fernet needs bytes.
# We will use the FERNET_KEY to lock the plaintext data (like email and phone) into an unreadable, scrambled format 
# (email_enc, phone_enc). It's the only key that can unlock that data to get the original plaintext back.
# We will do this because we want to store PII (Personally Identifiable Information) securely.

BLIND_INDEX_KEY = base64.b64decode(os.environ["BLIND_INDEX_KEY"])
# This is a base64-decoded key used to create blind indexes for searchable fields (like email_bidx).
# A blind index is a hashed version of a field that allows searching without revealing the original value.
# We will use this to create blind indexes for fields like email, so we can search for users by email without 
# storing the actual email in plaintext. This enhances privacy while still allowing efficient lookups and
# helps comply with data protection regulations. It also helps with hackers breaching your database and stealing user data.

#--------------------------------------------------------------------------------------------------------
# Password hashing (Argon2id)
#--------------------------------------------------------------------------------------------------------
# Argon2id is a specific "scrambling recipe" (algorithm). It's considered one of the best and most
# secure password hashing algorithms available today. It's famous for being deliberately difficult for attackers. 
# Its key features are:
ph = PasswordHasher(time_cost=3, memory_cost=65536, parallelism=1)  # ~64MB per hash

# time_cost=3: The algorithm will perform 3 iterations of its hashing process, making it slower and more resistant to brute-force attacks.
# Three iterations means that it takes the result of the previous hash and hash it again (three times total).

# memory_cost=65536: It will use 64MB of RAM during the hashing operation of each password. This means that inside the
# 64MB of RAM, it will do many complex operations to scramble the password (three times, because time_cost=3).
# This high memory usage makes it harder for attackers to use specialized hardware (like GPUs or ASICs) to crack passwords quickly.

# parallelism=1: This is how many threads in the CPU (workers) it will use to do the hashing. 1 means it will use a single thread.
# Usuing 1 thread means that the attacker, to make even one guess at this password, they must be able to use 64MB of RAM in a single thread.
# This makes it harder for hackers to parallelize their attacks across multiple threads or cores.

#--------------------------------------------------------------------------------------------------------
# Hash password function that adds PEPPER and hashes with Argon2id
#--------------------------------------------------------------------------------------------------------
def hash_password(pw: str) -> str:
    # add PEPPER (secret spice) before hashing
    return ph.hash(pw + PEPPER)
# This function takes a plaintext password (pw), appends the secret PEPPER to it, and then hashes the combined string using Argon2id.
# The result is a secure hash that can be stored safely in the database instead of the plaintext password.
#--------------------------------------------------------------------------------------------------------

# ---- Encryption for PII (Fernet: AES + HMAC)
fernet = Fernet(FERNET_KEY)
# It's this fernet object that you'll use in the next step to actually encrypt and decrypt data.

def encrypt_str(s: str) -> str:
    return fernet.encrypt(s.encode()).decode()
# This function takes a plaintext string (s), encodes it to bytes, encrypts it using the Fernet symmetric encryption,
# and then decodes the encrypted bytes back to a string for easy storage.
# The result is an encrypted version of the original string that can be safely stored in the database

# Normalize email for blind indexing
def normalize_email(s: str) -> str:
    return s.strip().lower()
# s.strip(): This removes any accidental whitespace (spaces, tabs, newlines) from the beginning or end of the string.
# .lower(): This converts all characters in the email to lowercase. This is important because email addresses are case-insensitive.


def blind_index(s: str) -> str:
    return hmac.new(BLIND_INDEX_KEY, s.encode(), hashlib.sha256).hexdigest()
# This function creates a secure, searchable, one-way fingerprint (a "blind index") for a piece of data.
# s.encode(): This takes your input string (the normalized email) and converts it into bytes (e.g., b'user@example.com'), which is the format all crypto functions need.
# hmac.new(...): This creates a special, keyed hash called an HMAC. Think of it as a "secret fingerprint" generator. It takes three things:
# (1) The Key: BLIND_INDEX_KEY (Your secret bytes. This is the "secret" part.)
# (2) The Message: s.encode() (The data you want to fingerprint.)
# (3) The Recipe: hashlib.sha256 (The hashing algorithm you want to use.)
# .hexdigest(): This converts the resulting HMAC into a readable hexadecimal string (like '5f4dcc3b5aa765d61d8327deb882cf99').
# The output is a fixed-length string that represents the original data but cannot be reversed to get the original data back.
# This blind index can be stored and searched in your database without exposing the actual sensitive data.

#--------------------------------------------------------------------------------------------------------
# Main job function
#--------------------------------------------------------------------------------------------------------
def main() -> None:
    #--------------------------------------------------------------------------------------------------------
    # Fetch with a small timeout and fail clearly if HTTP != 200
    #--------------------------------------------------------------------------------------------------------
    # Sends an HTTP GET to Random User API asking for 10 users and only the listed fields.
    resp = requests.get(
        "https://randomuser.me/api/?results=10&inc=login,name,email,registered,dob,location,phone",
        # timeout=15 ensures the request doesn’t hang forever (cron-friendly).
        # It means: "If the server doesn't respond to me within 15 seconds, just give up, and tell me you failed."
        timeout=15,
    )

    resp.raise_for_status()
    # This line means: If the server returned an error code (4xx/5xx), raise an exception now.
    # This way the job fails fast instead of writing bad data.

    #--------------------------------------------------------------------------------------------------------
    # Parses the JSON body (resp.json()) and extracts the results array (the list of user objects the API returns).
    #--------------------------------------------------------------------------------------------------------
    users = resp.json()["results"]  # "results" is the Json object that the API says it has all the user info.

    #--------------------------------------------------------------------------------------------------------
    # Turns the list of nested user dicts into a flat DataFrame (columns like name.first, location.country, etc.).
    #--------------------------------------------------------------------------------------------------------
    df = pd.json_normalize(users)

    #--------------------------------------------------------------------------------------------------------
    # These lines create a data folder inside this project directory and then define the full path to the 
    # random_users.csv file inside it.
    #--------------------------------------------------------------------------------------------------------
    DATA_DIR = BASE_DIR / "data"
    DATA_DIR.mkdir(exist_ok=True)
    csv_file = DATA_DIR / "random_users.csv"

    # --------------------------------------------------------------------------------------------------------
    # Selects only the columns you care about into a new DataFrame called main_cols.
    # --------------------------------------------------------------------------------------------------------
    main_cols = df[[
        "login.uuid","name.first","name.last",
        "email","dob.date","dob.age",
        "location.country","location.street.name",
        "login.username","login.password",
        "phone"
    ]].copy()

    # Explanation of .copy():
    # As a rule of thumb, everytime you select a subset of columns from a DataFrame and you "intend to modify that subset", 
    # add .copy() to make it an independent DataFrame. This tells Pandas your clear intention: "I am creating a new, 
    # separate table, and I will be changing it."
    # The only trade-off is that it uses a bit more memory, but for data from an API (which is usually small, like our 10 rows),
    # the safety and clarity are far more important.

    #--------------------------------------------------------------------------------------------------------
    # Hash and encrypt sensitive fields, creating new columns for the hashed/encrypted data.
    #--------------------------------------------------------------------------------------------------------
    # .loc : This is a powerful Pandas tool for selecting and modifying specific parts of a DataFrame.
    # It uses two main inputs: rows and columns, in the format .loc[rows, columns].
    # The colon (:) means "all". So when you see main_cols.loc[:, "columnName"], it means "for all rows, get this specific column".

    main_cols.loc[:, "password_hash"]   = main_cols["login.password"].apply(hash_password)
    main_cols.loc[:, "email_enc"]       = main_cols["email"].apply(encrypt_str)
    main_cols.loc[:, "phone_enc"]       = main_cols["phone"].apply(encrypt_str)
    main_cols.loc[:, "street_name_enc"] = main_cols["location.street.name"].apply(encrypt_str)
    main_cols.loc[:, "email_bidx"]      = main_cols["email"].apply(lambda s: blind_index(normalize_email(s)))

    # .apply(). This is a Pandas command that says: "Go to this column, and run this specific function on every single row."
    # .apply() can only accept one function, but for the email_bidx column you needed to do two things: first normalize_email
    # and then blind_index. To do this, you use a lambda function, which is a small, anonymous function defined on the fly.
    # The lambda function takes an input s (the email), normalizes it, and then creates the blind index from the normalized email.

    #--------------------------------------------------------------------------------------------------------
    # drop plaintext secrets
    main_cols.drop(
        columns=["login.password","email","phone","location.street.name"],
        inplace=True
    )
    # inplace=True means: "Make this change directly to main_cols DataFrame. Don't create a new DataFrame."

    #--------------------------------------------------------------------------------------------------------
    # Append to CSV (and really drop duplicates by unique user id)
    #--------------------------------------------------------------------------------------------------------
    if csv_file.exists():
        df_existing = pd.read_csv(csv_file)
        df_final = pd.concat([df_existing, main_cols], ignore_index=True)
    else:
        df_final = main_cols

    # ignore_index=True means: "After combining these two tables, reset the row numbers to be a simple sequence from 0 to N-1."
    # Without ignore_index=True (The Messy Way): Pandas keeps the original page numbers. Your new, combined table would have
    # a messy index: 0, 1, ... 99 (from the old data) ...and then... 0, 1, ... 9 (from the new data)

    #--------------------------------------------------------------------------------------------------------
    # Removes duplicate users based on their unique id (login.uuid), keeping the first occurrence.
    #--------------------------------------------------------------------------------------------------------
    df_final = df_final.drop_duplicates(subset=["login.uuid"])

    #--------------------------------------------------------------------------------------------------------
    # Writes the final table back to random_users.csv. index=False avoids writing a numeric index column.
    #--------------------------------------------------------------------------------------------------------
    df_final.to_csv(csv_file, index=False)

    #--------------------------------------------------------------------------------------------------------
    # Print summary info for logging/monitoring
    #--------------------------------------------------------------------------------------------------------
    print(f"Wrote {len(df_final)} rows to {csv_file}")
    print(f"Api_url=https://randomuser.me ...")
    print(f"Http_status={resp.status_code}")
    print(f"Retries_used={resp.raw.retries.total if resp.raw.retries else 0}")
    print(f"Rows_fetched={len(df)}")
    print(f"Rows_after_dedup={len(df_final)}")
    print(f"Output={csv_file}")
    #--------------------------------------------------------------------------------------------------------


# Only run the job when this file is executed directly (not when it's imported)
if __name__ == "__main__":
    main()
