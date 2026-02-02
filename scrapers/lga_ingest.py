import pandas as pd
from sqlalchemy import create_engine, text
import os
import json

# Configuration
BASE_DIR = os.getcwd()
DATA_PROCESSED = os.path.join(BASE_DIR, 'data', 'processed')
DB_PATH = os.path.join(DATA_PROCESSED, 'victoria_sim.db')
PROJECT_LOG_PATH = os.path.join(BASE_DIR, 'project_log.json')
DATABASE_URL = f"sqlite:///{DB_PATH}"

# Representative Data (Approximated for Simulation Seeding)
lga_data = [
    # Hobsons Bay (Metro West)
    {"lga_name": "Hobsons Bay", "year": 2020, "population": 95000, "median_house_price": 950000.0, "political_lean": "Labor"},
    {"lga_name": "Hobsons Bay", "year": 2024, "population": 98000, "median_house_price": 1050000.0, "political_lean": "Labor"},
    
    # Melbourne City (Metro Central)
    {"lga_name": "Melbourne", "year": 2020, "population": 150000, "median_house_price": 900000.0, "political_lean": "Green"},
    {"lga_name": "Melbourne", "year": 2024, "population": 165000, "median_house_price": 920000.0, "political_lean": "Green"},
    
    # Greater Geelong (Regional Hub)
    {"lga_name": "Greater Geelong", "year": 2020, "population": 260000, "median_house_price": 750000.0, "political_lean": "Labor/Ind"},
    {"lga_name": "Greater Geelong", "year": 2024, "population": 285000, "median_house_price": 850000.0, "political_lean": "Labor/Ind"},
    
    # Greater Bendigo (Regional Hub)
    {"lga_name": "Greater Bendigo", "year": 2020, "population": 118000, "median_house_price": 550000.0, "political_lean": "Labor-swing"},
    {"lga_name": "Greater Bendigo", "year": 2024, "population": 126000, "median_house_price": 650000.0, "political_lean": "Labor-swing"},
    
    # Ballarat (Regional Hub)
    {"lga_name": "Ballarat", "year": 2020, "population": 109000, "median_house_price": 530000.0, "political_lean": "Labor"},
    {"lga_name": "Ballarat", "year": 2024, "population": 117000, "median_house_price": 620000.0, "political_lean": "Labor"},
    
    # Casey (Growth Corridor SE)
    {"lga_name": "Casey", "year": 2020, "population": 360000, "median_house_price": 780000.0, "political_lean": "Liberal-swing"},
    {"lga_name": "Casey", "year": 2024, "population": 400000, "median_house_price": 880000.0, "political_lean": "Liberal-swing"},
    
    # Wyndham (Growth Corridor West)
    {"lga_name": "Wyndham", "year": 2020, "population": 280000, "median_house_price": 650000.0, "political_lean": "Labor"},
    {"lga_name": "Wyndham", "year": 2024, "population": 320000, "median_house_price": 720000.0, "political_lean": "Labor"},
    
    # Wodonga (Regional Border)
    {"lga_name": "Wodonga", "year": 2020, "population": 41000, "median_house_price": 450000.0, "political_lean": "Coalition"},
    {"lga_name": "Wodonga", "year": 2024, "population": 44000, "median_house_price": 580000.0, "political_lean": "Coalition"},
    
    # Latrobe (Regional Industry)
    {"lga_name": "Latrobe", "year": 2020, "population": 75000, "median_house_price": 380000.0, "political_lean": "Coalition"},
    {"lga_name": "Latrobe", "year": 2024, "population": 77000, "median_house_price": 460000.0, "political_lean": "Coalition"},
    
    # Mildura (Regional North)
    {"lga_name": "Mildura", "year": 2020, "population": 55000, "median_house_price": 360000.0, "political_lean": "Coalition/Ind"},
    {"lga_name": "Mildura", "year": 2024, "population": 57000, "median_house_price": 440000.0, "political_lean": "Coalition/Ind"}
]

def ingest_lgas():
    print("--- Starting LGA Data Ingest ---")
    engine = create_engine(DATABASE_URL)
    
    df = pd.DataFrame(lga_data)
    
    # Upsert/Replace logic
    # We will clear table for clean snapshot as we are "Populating"
    with engine.connect() as conn:
        trans = conn.begin()
        try:
            conn.execute(text("DELETE FROM lga_stats"))
            trans.commit()
            print("Cleared existing lga_stats table.")
        except Exception as e:
            print(f"Error clearing table: {e}")
            trans.rollback()

    df.to_sql('lga_stats', engine, if_exists='append', index=False)
    print(f"Inserted {len(df)} LGA records.")
    
    # Verification Snapshot
    print("\nXXX Regional Snapshot: 2020 to 2024 XXX")
    print(f"{'LGA':<20} | {'Pop Growth':<12} | {'Price Growth':<12} | {'Lean'}")
    print("-" * 65)
    
    lgas = df['lga_name'].unique()
    for lga in lgas:
        d20 = df[(df['lga_name'] == lga) & (df['year'] == 2020)].iloc[0]
        d24 = df[(df['lga_name'] == lga) & (df['year'] == 2024)].iloc[0]
        
        pop_growth = ((d24['population'] - d20['population']) / d20['population']) * 100
        price_growth = ((d24['median_house_price'] - d20['median_house_price']) / d20['median_house_price']) * 100
        
        print(f"{lga:<20} | {pop_growth:>10.1f}% | {price_growth:>10.1f}% | {d24['political_lean']}")

    update_log()

def update_log():
    try:
        with open(PROJECT_LOG_PATH, 'r') as f:
            log_data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        log_data = {}
    
    log_data['lga_granularity_initialized'] = True
    
    with open(PROJECT_LOG_PATH, 'w') as f:
        json.dump(log_data, f, indent=2)
    print(f"\nUpdated {PROJECT_LOG_PATH}")

if __name__ == "__main__":
    ingest_lgas()
