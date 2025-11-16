#!/usr/bin/env python3
"""
Pandas transformations for the RandomUser data:
- flatten JSON
- select columns
- hash/encrypt PII
- drop plaintext columns
"""

from typing import List, Dict, Tuple
import pandas as pd
from .crypto_utils import (
    hash_password,
    encrypt_str,
    normalize_email,
    blind_index,
)


def transform_users(users: List[Dict]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    # This function will return two DataFrames: (df_raw, df_secure)
    # df_raw: fully flattened DataFrame of all user data
    # df_secure: selected columns with hashed/encrypted PII (no plaintext secrets)

    df_raw = pd.json_normalize(users) # Turns the list of nested user dicts into a flat DataFrame (columns like name.first, location.country, etc.)
    

    # Select only the columns we care about into a new DataFrame.
    df_secure = df_raw[
        [
            "login.uuid",
            "name.first",
            "name.last",
            "email",
            "dob.date",
            "dob.age",
            "location.country",
            "location.street.name",
            "login.username",
            "login.password",
            "phone",
        ]
    ].copy()

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

    df_secure.loc[:, "password_hash"] = df_secure["login.password"].apply(hash_password)
    df_secure.loc[:, "email_enc"] = df_secure["email"].apply(encrypt_str)
    df_secure.loc[:, "phone_enc"] = df_secure["phone"].apply(encrypt_str)
    df_secure.loc[:, "street_name_enc"] = df_secure["location.street.name"].apply(encrypt_str)
    df_secure.loc[:, "email_bidx"] = df_secure["email"].apply(lambda s: blind_index(normalize_email(s)))

    # .apply(). This is a Pandas command that says: "Go to this column, and run this specific function on every single row."
    # .apply() can only accept one function, but for the email_bidx column you needed to do two things: first normalize_email
    # and then blind_index. To do this, you use a lambda function, which is a small, anonymous function defined on the fly.
    # The lambda function takes an input s (the email), normalizes it, and then creates the blind index from the normalized email.


    # Drop plaintext secrets: we keep only the secure versions.
    df_secure.drop(
        columns=["login.password", "email", "phone", "location.street.name"],
        inplace=True,
    )
    # inplace=True means: "Make this change directly to main_cols DataFrame. Don't create a new DataFrame."

    return df_raw, df_secure
