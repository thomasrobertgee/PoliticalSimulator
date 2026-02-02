import pandas as pd
import os
import json
from sqlalchemy import create_engine, text

# Configuration
BASE_DIR = os.getcwd() # Assumes running from project root
DATA_RAW = os.path.join(BASE_DIR, 'data', 'raw')
DATA_PROCESSED = os.path.join(BASE_DIR, 'data', 'processed')
DB_PATH = os.path.join(DATA_PROCESSED, 'victoria_sim.db')
PROJECT_LOG_PATH = os.path.join(BASE_DIR, 'project_log.json')
MANUAL_FILE = os.path.join(DATA_RAW, 'historical_manual.csv')

def ingest_data():
    print("--- Starting Economic Data Ingest ---")
    
    # 1. Attempt Import
    if os.path.exists(MANUAL_FILE):
        print(f"Found manual data file: {MANUAL_FILE}")
        df = pd.read_csv(MANUAL_FILE)
    else:
        print("Error: Manual data file not found and automated download not available.")
        return

    # 2. Data Cleaning
    print("Cleaning data...")
    # Ensure types
    df['year'] = df['year'].astype(int)
    df['gsp_billions'] = df['gsp_billions'].astype(float)
    df['unemployment_rate'] = df['unemployment_rate'].astype(float)
    df['state_debt_billions'] = df['state_debt_billions'].astype(float)
    df['population_millions'] = df['population_millions'].astype(float)

    # 3. Database Upsert
    database_url = f"sqlite:///{DB_PATH}"
    engine = create_engine(database_url)
    
    print(f"Connecting to database: {DB_PATH}")
    
    # Get existing years to avoid duplicates
    try:
        existing_years_df = pd.read_sql("SELECT year FROM economic_indicators", engine)
        existing_years = set(existing_years_df['year'].tolist())
    except Exception as e:
        print(f"Warning: Could not query existing data (Table might be empty): {e}")
        existing_years = set()

    # Filter out existing years
    new_data = df[~df['year'].isin(existing_years)]

    if not new_data.empty:
        print(f"Inserting {len(new_data)} new rows for years: {new_data['year'].tolist()}")
        new_data.to_sql('economic_indicators', engine, if_exists='append', index=False)
    else:
        print("No new data to insert.")

    # 4. Summary & Logging
    # Get all years now in DB
    final_df = pd.read_sql("SELECT * FROM economic_indicators ORDER BY year ASC", engine)
    
    oldest = final_df.iloc[0]
    newest = final_df.iloc[-1]
    
    print("\n--- Data Summary ---")
    print(f"Total Records: {len(final_df)}")
    print(f"Oldest Entry ({int(oldest['year'])}): GSP=${oldest['gsp_billions']}B, Unemp={oldest['unemployment_rate']}%")
    print(f"Newest Entry ({int(newest['year'])}): GSP=${newest['gsp_billions']}B, Unemp={newest['unemployment_rate']}%")
    
    # Update project_log.json
    update_log(final_df['year'].tolist())

def update_log(years_list):
    try:
        with open(PROJECT_LOG_PATH, 'r') as f:
            log_data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        log_data = {}
    
    log_data['ingested_years'] = sorted(list(set(years_list)))
    
    with open(PROJECT_LOG_PATH, 'w') as f:
        json.dump(log_data, f, indent=2)
    print(f"\nUpdated {PROJECT_LOG_PATH}")

if __name__ == "__main__":
    ingest_data()
