#!/usr/bin/env python3

# ------------------------------------------------------------------------------------------
# --------------------- HTTP client for the RandomUser API ----------------------------------
# ------------------------------------------------------------------------------------------
from typing import Tuple, List, Dict
import requests

API_URL = (
    "https://randomuser.me/api/"
    "?results=10"
    "&inc=login,name,email,registered,dob,location,phone"
)

def fetch_random_users(timeout: int = 15) -> Tuple[List[Dict], requests.Response]:
    # This function will return a tuple: (list of users, original response object)
    # The list of users is a list of dictionaries, where each dictionary represents a user.
    # The original response object is useful for checking status codes, headers, etc.

    resp = requests.get(API_URL, timeout=timeout) 
    # Sends an HTTP GET to Random User API asking for 10 users and only the listed fields. timeout=ti
    resp.raise_for_status() 
    # This line means: If the server returned an error code (4xx/5xx), raise an exception now. This way the job fails fast instead of writing bad data.
    
    data = resp.json() # Parses the JSON body (resp.json())
    users = data["results"] # Extracts the "results" array (the list of user objects the API returns).
    return users, resp # Return both users and the original response object
