import pandas as pd
from sqlalchemy import create_engine, text
import numpy as np
import os
import random

# Configuration
BASE_DIR = os.getcwd()
DB_PATH = os.path.join(BASE_DIR, 'data', 'processed', 'victoria_sim.db')
DATABASE_URL = f"sqlite:///{DB_PATH}"

# --- 1. Infrastructure Projects ---
# 10 Major Projects (Mix of historic and future)
infra_data = [
    {"name": "CityLink", "start_year": 1996, "completion_year": 2000, "budget_billions": 2.2, "region_impacted": "Melbourne"},
    {"name": "Regional Fast Rail", "start_year": 2000, "completion_year": 2006, "budget_billions": 0.75, "region_impacted": "Regional"},
    {"name": "EastLink", "start_year": 2005, "completion_year": 2008, "budget_billions": 2.5, "region_impacted": "Melbourne East"},
    {"name": "Desalination Plant", "start_year": 2009, "completion_year": 2012, "budget_billions": 3.5, "region_impacted": "Gippsland/State"},
    {"name": "Peninsula Link", "start_year": 2010, "completion_year": 2013, "budget_billions": 0.85, "region_impacted": "Mornington Peninsula"},
    {"name": "Metro Tunnel", "start_year": 2018, "completion_year": 2025, "budget_billions": 14.0, "region_impacted": "Melbourne CBD"},
    {"name": "West Gate Tunnel", "start_year": 2018, "completion_year": 2025, "budget_billions": 10.0, "region_impacted": "Melbourne West"},
    {"name": "Level Crossing Removals (Phase 1)", "start_year": 2015, "completion_year": 2025, "budget_billions": 8.0, "region_impacted": "Metro All"},
    {"name": "North East Link", "start_year": 2021, "completion_year": 2028, "budget_billions": 16.0, "region_impacted": "Melbourne North"},
    {"name": "Suburban Rail Loop (East)", "start_year": 2022, "completion_year": 2035, "budget_billions": 35.0, "region_impacted": "Middle Ring"},
    {"name": "Airport Rail", "start_year": 2024, "completion_year": 2031, "budget_billions": 13.0, "region_impacted": "Melbourne NW"}
]

# --- 2. Social Indicators (2005 - 2025) ---
# Back-filling 20 years
def generate_social_data():
    years = np.arange(2005, 2026)
    data = []
    
    for year in years:
        # Crime Rate (per 100k) - fluctuating around 5000-6000
        # Spike in 2016-17? 
        base_crime = 5500
        crime_noise = np.random.uniform(-300, 300)
        # Trend: rising slightly lately
        trend = (year - 2005) * 20
        crime = base_crime + trend + crime_noise
        if year in [2020, 2021]: crime -= 500 # Lockdowns reduced some crimes stats
        
        # Education Rank (Global/National) - Lower is better. 1 is best.
        # Vic usually high. 
        edu = 3 if year < 2015 else 4
        
        # Health Satisfaction (0-100)
        # Generally high, dipping in COVID
        health = 75.0 + np.random.uniform(-2, 2)
        if year in [2020, 2021, 2022]: health -= 8.0 # System strain
        
        data.append({
            "year": int(year),
            "crime_rate_per_100k": round(crime, 1),
            "education_rank": edu,
            "health_satisfaction_score": round(health, 1)
        })
    return pd.DataFrame(data)

def ingest_pillars():
    print("--- Starting Pillars Ingest ---")
    engine = create_engine(DATABASE_URL)
    
    # 1. Infrastructure
    infra_df = pd.DataFrame(infra_data)
    with engine.connect() as conn:
        trans = conn.begin()
        conn.execute(text("DELETE FROM infrastructure_projects"))
        trans.commit()
    infra_df.to_sql('infrastructure_projects', engine, if_exists='append', index=False)
    print("Infrastructure Projects Ingested.")

    # 2. Social
    social_df = generate_social_data()
    with engine.connect() as conn:
        trans = conn.begin()
        conn.execute(text("DELETE FROM social_indicators"))
        trans.commit()
    social_df.to_sql('social_indicators', engine, if_exists='append', index=False)
    print("Social Indicators Ingested (2005-2025).")

if __name__ == "__main__":
    ingest_pillars()
