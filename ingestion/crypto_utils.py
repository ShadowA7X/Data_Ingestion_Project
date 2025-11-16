#!/usr/bin/env python3
"""
Cryptography helpers: password hashing, encryption, normalization, blind indexes.
"""
# --------------------------------------------------------------------------------------------------------
# Cryptography imports
# --------------------------------------------------------------------------------------------------------
import base64, hmac, hashlib # built-in modules for encoding, hashing, and cryptographic operations.
from argon2 import PasswordHasher # It provides secure password hashing using the Argon2 algorithm.
from cryptography.fernet import Fernet # It provides easy-to-use symmetric encryption and decryption for sensitive data.

# --------------------------------------------------------------------------------------------------------
import os #for filesystem operations and environment variables
# Python uses the os tool to interact with the operating system. We use the os module whenever we need our
# script to read or change things at the operating system level (like change directories, environment variables, etc.).
# Use os for other OS tasks that pathlib doesn't do.
# --------------------------------------------------------------------------------------------------------

from pathlib import Path # We use Path objects to handle file and directory paths. Use pathlib for all path-related tasks.
from dotenv import load_dotenv # It loads environment variables from a .env file into your script's environment.

# -----------------------------------------------------------------------------
# Load secrets from .env (one directory above this file)
# -----------------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent.parent  
# Get the parent directory (Data_Ingestion) of the current file's directory (ingestion).
load_dotenv(BASE_DIR / ".env") # This read your secret .env file and load those secrets into the script's environment.

# ---------- Reading secret keys from environment variables -----------------------------
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

# -----------------------------------------------------------------------------
# Encryption for PII (Fernet: AES + HMAC)
# -----------------------------------------------------------------------------

fernet = Fernet(FERNET_KEY)
# It's this fernet object that you'll use in the next step to actually encrypt and decrypt data.

def encrypt_str(s: str) -> str:
    return fernet.encrypt(s.encode()).decode()
# This function takes a plaintext string (s), encodes it to bytes, encrypts it using the Fernet symmetric encryption,
# and then decodes the encrypted bytes back to a string for easy storage.
# The result is an encrypted version of the original string that can be safely stored in the database

# -----------------------------------------------------------------------------
# Blind index helpers
# -----------------------------------------------------------------------------
# Normalize email by trimming whitespace and converting to lowercase
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
