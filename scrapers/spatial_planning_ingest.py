import pandas as pd
from sqlalchemy import create_engine, text
import numpy as np
import os

# Configuration
BASE_DIR = os.getcwd()
DB_PATH = os.path.join(BASE_DIR, 'data', 'processed', 'victoria_sim.db')
DATABASE_URL = f"sqlite:///{DB_PATH}"

LGAS = [
    "Hobsons Bay", "Melbourne", "Greater Geelong", "Greater Bendigo", "Ballarat",
    "Casey", "Wyndham", "Wodonga", "Latrobe", "Mildura"
]

def generate_land_use():
    data = []
    # Approx zoning splits
    profiles = {
        "Hobsons Bay": {"Res": 50, "Ind": 30, "Green": 5, "Comm": 5, "Other": 10}, # Heavy industry
        "Melbourne": {"Res": 30, "Ind": 5, "Green": 10, "Comm": 40, "Other": 15}, # CBD
        "Greater Geelong": {"Res": 60, "Ind": 15, "Green": 15, "Comm": 5, "Other": 5},
        "Casey": {"Res": 70, "Ind": 10, "Green": 15, "Comm": 5, "Other": 0}, # Suburbia
        "Latrobe": {"Res": 40, "Ind": 30, "Green": 20, "Comm": 5, "Other": 5}, # Power/Ind
    }
    # Default for others
    default_p = {"Res": 60, "Ind": 10, "Green": 20, "Comm": 5, "Other": 5}
    
    for lga in LGAS:
        p = profiles.get(lga, default_p)
        for z, val in p.items():
            data.append({"lga_name": lga, "zone_type": z, "percentage_coverage": val})
            
    return pd.DataFrame(data)

def generate_housing_diversity():
    years = np.arange(2011, 2026)
    data = []
    
    for lga in LGAS:
        # Trends 2011 -> 2025
        # Melbourne: High Apt
        # Hobsons Bay: Med Apt growth
        # Casey: Low Apt, High House
        
        base_house = 80.0
        base_apt = 5.0
        base_town = 15.0
        
        if lga == "Melbourne":
            base_house = 5.0
            base_apt = 85.0
            base_town = 10.0
        elif lga == "Hobsons Bay":
            base_house = 70.0
            base_apt = 10.0
            base_town = 20.0
        elif lga in ["Wyndham", "Casey"]:
            base_house = 90.0
            base_apt = 2.0
            base_town = 8.0
            
        for year in years:
            progress = (year - 2011) / 14.0
            
            # General trend: Densification
            shift = 5.0 * progress
            if lga == "Melbourne": shift = 0 # Already dense
            
            house = base_house - shift
            apt = base_apt + (shift * 0.4)
            town = base_town + (shift * 0.6)
            
            # Normalize
            total = house + apt + town
            house = (house / total) * 100
            apt = (apt / total) * 100
            town = (town / total) * 100
            
            data.append({
                "year": int(year),
                "lga_name": lga,
                "standalone_house_pct": round(house, 1),
                "apartment_pct": round(apt, 1),
                "townhouse_pct": round(town, 1)
            })
    return pd.DataFrame(data)

def generate_employment_hubs():
    data = [
        {"hub": "Port of Melbourne", "lga": "Melbourne", "jobs": 30000, "ind": "Logistics"},
        {"hub": "Melbourne CBD", "lga": "Melbourne", "jobs": 450000, "ind": "Finance/Services"},
        {"hub": "Altona Refinery/Ind", "lga": "Hobsons Bay", "jobs": 15000, "ind": "Manufacturing"},
        {"hub": "Monash Cluster", "lga": "Monash", "jobs": 80000, "ind": "Tech/Edu"}, # Adding Monash Lga logic implicitly or mapping
        {"hub": "Avalon Airport", "lga": "Greater Geelong", "jobs": 5000, "ind": "Transport"},
        {"hub": "Geelong Refinery", "lga": "Greater Geelong", "jobs": 2000, "ind": "Manufacturing"},
        {"hub": "Bendigo Health", "lga": "Greater Bendigo", "jobs": 4000, "ind": "Health"},
        {"hub": "Latrobe Valley Power", "lga": "Latrobe", "jobs": 3000, "ind": "Utility"}
    ]
    # Filter to known LGAs to align FKs if enforced (SQLite usually loose but good practice)
    # Monash not in our 10 list, map to closest or skip. We'll map to 'Melbourne' for simplicity or skip.
    
    clean_data = []
    for d in data:
        if d['lga'] in LGAS:
            clean_data.append({
                "hub_name": d['hub'],
                "lga_name": d['lga'],
                "estimated_jobs": d['jobs'],
                "primary_industry": d['ind']
            })
            
    return pd.DataFrame(clean_data)

def ingest_spatial():
    print("--- Starting Spatial & Planning Ingest ---")
    engine = create_engine(DATABASE_URL)
    
    # 1. Land Use
    lu_df = generate_land_use()
    with engine.connect() as conn:
        trans = conn.begin()
        conn.execute(text("DELETE FROM land_use_zones"))
        trans.commit()
    lu_df.to_sql('land_use_zones', engine, if_exists='append', index=False)
    print("Land Use Zones Ingested.")

    # 2. Diversity
    div_df = generate_housing_diversity()
    with engine.connect() as conn:
        trans = conn.begin()
        conn.execute(text("DELETE FROM housing_diversity"))
        trans.commit()
    div_df.to_sql('housing_diversity', engine, if_exists='append', index=False)
    print("Housing Diversity Ingested.")

    # 3. Hubs
    hub_df = generate_employment_hubs()
    with engine.connect() as conn:
        trans = conn.begin()
        conn.execute(text("DELETE FROM employment_hubs"))
        trans.commit()
    hub_df.to_sql('employment_hubs', engine, if_exists='append', index=False)
    print("Employment Hubs Ingested.")

if __name__ == "__main__":
    ingest_spatial()
