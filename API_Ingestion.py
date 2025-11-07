#!/usr/bin/env python3
import os #for filesystem/path ops
from pathlib import Path
import requests # to call the HTTP API
import pandas as pd # for working with tabular data & CSVs
from dotenv import load_dotenv
import base64, hmac, hashlib
from argon2 import PasswordHasher
from cryptography.fernet import Fernet

#--------------------------------------------------------------------------------------------------------
# Work relative to this file (repo portability)
#--------------------------------------------------------------------------------------------------------
# Where your script lives (…/Data_Ingestion)
BASE_DIR = Path(__file__).resolve().parent
os.chdir(BASE_DIR)

load_dotenv() # will load ./.env since we chdir(BASE_DIR)
print("Loaded .env from project folder")  # optional sanity check


#--------------------------------------------------------------------------------------------------------
# Helper Hash and encryption
#--------------------------------------------------------------------------------------------------------

# ---- Secrets from .env
PEPPER = os.environ["PEPPER"]
FERNET_KEY = os.environ["FERNET_KEY"].encode()
BLIND_INDEX_KEY = base64.b64decode(os.environ["BLIND_INDEX_KEY"])

# ---- Password hashing (Argon2id)
ph = PasswordHasher(time_cost=3, memory_cost=65536, parallelism=1)  # ~64MB per hash

def hash_password(pw: str) -> str:
    # add PEPPER (secret spice) before hashing
    return ph.hash(pw + PEPPER)

# ---- Encryption for PII (Fernet: AES + HMAC)
fernet = Fernet(FERNET_KEY)

def encrypt_str(s: str) -> str:
    return fernet.encrypt(s.encode()).decode()

# ---- Blind index (searchable, no plaintext)
def normalize_email(s: str) -> str:
    return s.strip().lower()

def blind_index(s: str) -> str:
    return hmac.new(BLIND_INDEX_KEY, s.encode(), hashlib.sha256).hexdigest()

#--------------------------------------------------------------------------------------------------------
# Fetch with a small timeout and fail clearly if HTTP != 200
#--------------------------------------------------------------------------------------------------------
#Sends an HTTP GET to Random User API asking for 10 users and only the listed fields.
resp = requests.get(
    "https://randomuser.me/api/?results=10&inc=login,name,email,registered,dob,location,phone",
    # timeout=15 ensures the request doesn’t hang forever (cron-friendly).
    timeout=15,
)

resp.raise_for_status()
# This line means: If the server returned an error code (4xx/5xx), raise an exception now.
# This way the job fails fast instead of writing bad data.

#--------------------------------------------------------------------------------------------------------
# Parses the JSON body (resp.json()) and extracts the results array (the list of user objects the API returns).
#--------------------------------------------------------------------------------------------------------
users = resp.json()["results"] # "results" is the Json object that the API says it has all the user info.

#--------------------------------------------------------------------------------------------------------
# Turns the list of nested user dicts into a flat DataFrame (columns like name.first, location.country, etc.).
#--------------------------------------------------------------------------------------------------------

df = pd.json_normalize(users)

#--------------------------------------------------------------------------------------------------------
# Selects only the columns you care about into a new DataFrame called main_cols.
#--------------------------------------------------------------------------------------------------------

main_cols = df[[
    "login.uuid","name.first","name.last",
    "email","dob.date","dob.age",
    "location.country","location.street.name",
    "login.username","login.password",
    "phone"
]]

#--------------------------------------------------------------------------------------------------------
# Put artifacts inside the repo
#--------------------------------------------------------------------------------------------------------

DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)
csv_file = DATA_DIR / "random_users.csv"

# --------------------------------------------------------------------------------------------------------
# Apply hashing and encryption to sensitive columns
# --------------------------------------------------------------------------------------------------------

main_cols = df[[
    "login.uuid","name.first","name.last",
    "email","dob.date","dob.age",
    "location.country","location.street.name",
    "login.username","login.password",
    "phone"
]].copy()  # copy() avoids SettingWithCopy warnings

# derive secure columns
main_cols["password_hash"]   = main_cols["login.password"].apply(hash_password)
main_cols["email_enc"]       = main_cols["email"].apply(encrypt_str)
main_cols["phone_enc"]       = main_cols["phone"].apply(encrypt_str)
main_cols["street_name_enc"] = main_cols["location.street.name"].apply(encrypt_str)
main_cols["email_bidx"]      = main_cols["email"].apply(lambda s: blind_index(normalize_email(s)))

# drop plaintext secrets
main_cols.drop(
    columns=["login.password","email","phone","location.street.name"],
    inplace=True
)

#--------------------------------------------------------------------------------------------------------
# Append to CSV (and really drop duplicates by unique user id)
#--------------------------------------------------------------------------------------------------------

if csv_file.exists():
    df_existing = pd.read_csv(csv_file)
    df_final = pd.concat([df_existing, main_cols], ignore_index=True)
else:
    df_final = main_cols

#--------------------------------------------------------------------------------------------------------
# Removes duplicate users based on their unique id (login.uuid), keeping the first occurrence.
#--------------------------------------------------------------------------------------------------------
df_final = df_final.drop_duplicates(subset=["login.uuid"])

#--------------------------------------------------------------------------------------------------------
# Writes the final table back to random_users.csv. index=False avoids writing a numeric index column.
#--------------------------------------------------------------------------------------------------------
df_final.to_csv(csv_file, index=False)


print(f"wrote {len(df_final)} rows to {csv_file}")
print(f"api_url=https://randomuser.me ...")
print(f"http_status={resp.status_code} retries_used={resp.raw.retries.total if hasattr(resp.raw, 'retries') else 0}")
print(f"rows_fetched={len(df)} rows_after_dedup={len(df_final)} output={csv_file}")
