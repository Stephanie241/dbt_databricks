import requests
import pandas as pd
from datetime import datetime, timezone
import os

# Ensure seeds folder exists
os.makedirs("seeds", exist_ok=True)

API_URL = "https://api.fda.gov/drug/event.json?limit=50"

def fetch_health_data():
    response = requests.get(API_URL)
    response.raise_for_status()
    data = response.json()

    results = data.get("results", [])
    df = pd.json_normalize(results)

    # Rename columns to remove dots (SQL-safe)
    df.rename(columns={
        "patient.drug": "patient_drug",
        "patient.reaction": "patient_reaction"
    }, inplace=True)

    # Truncate large text columns to avoid dbt seed limit
    if 'patient_drug' in df.columns:
        df['patient_drug'] = df['patient_drug'].astype(str).str.slice(0, 1000)
    if 'patient_reaction' in df.columns:
        df['patient_reaction'] = df['patient_reaction'].astype(str).str.slice(0, 1000)

    # Add ingestion timestamp
    df['ingest_ts'] = datetime.now(timezone.utc)

    # Save CSV for dbt seed
    df.to_csv("seeds/health_data_raw.csv", index=False)
    print("Saved truncated CSV to seeds/health_data_raw.csv")
    return df

if __name__ == "__main__":
    fetch_health_data()
