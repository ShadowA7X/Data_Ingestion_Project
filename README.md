# 1_Data_Ingestion_Projects
This repository will be totally focused on mini examples of how to Ingest data from different resources

------------------------
API_Ingestion.py
------------------------

This file will create a CSV file using the https://randomuser.me/ API. Each time you execute this file, 10 new
records will be added to the CSV (If it previously exist. If not, It will create a new one)


-------------------------
Security measures
-------------------------

# macOS/Linux terminal — these just PRINT values
openssl rand -base64 32   # <- copy result for PEPPER
python -c 'from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())'  # <- copy for FERNET_KEY
openssl rand -base64 32   # <- copy result for BLIND_INDEX_KEY