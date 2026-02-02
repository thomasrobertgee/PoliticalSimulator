import pandas as pd
from sqlalchemy import create_engine, text
import numpy as np
import os

# Configuration
BASE_DIR = os.getcwd()
DB_PATH = os.path.join(BASE_DIR, 'data', 'processed', 'victoria_sim.db')
DATABASE_URL = f"sqlite:///{DB_PATH}"

def generate_environment_data():
    years = np.arange(1990, 2026)
    data = []
    
    for year in years:
        # Rainfall (avg ~600mm)
        rain = np.random.normal(600, 100)
        
        # Water Storage (Millennium Drought 1996-2010)
        if 1997 <= year <= 2009:
            water = 30 + np.random.uniform(0, 15) # Dire lows
            if year == 2009: water = 28.0 # Critical
        else:
            water = 60 + np.random.uniform(0, 25) # Recovery
        
        # Bushfires
        fires = 50000 + np.random.uniform(0, 50000) # Baseline
        if year == 2003: fires = 1300000 # Alpine fires
        if year == 2007: fires = 1100000 # Great Divide fires
        if year == 2009: fires = 450000 # Black Saturday (Intensity high, area moderate compared to others?) 
                                        # Actually Black Saturday area was ~450k ha but extreme intensity.
        if year == 2020: fires = 1500000 # Black Summer (Vic portion)
        
        data.append({
            "year": int(year),
            "avg_rainfall_mm": round(rain, 1),
            "melbourne_water_storage_pct": round(water, 1),
            "bushfire_area_hectares": round(fires, 0)
        })
    return pd.DataFrame(data)

def generate_health_edu_data():
    years = np.arange(1990, 2026)
    data = []
    
    for year in years:
        # Waitlist (Rising trend + COVID spike)
        base_wait = 30000 + ((year - 1990) * 1000)
        if year >= 2020: base_wait += 20000 # COVID backlog
        if year == 2024: base_wait += 10000 # Current crisis
        
        # Beds per 1000 (Declining efficiency/concentration?)
        # 1990: 4.0 -> 2024: 2.5
        beds = 4.0 - ((year - 1990) * 0.04)
        
        # School Enrollments
        # Correlates with population but younger cohort
        enroll = 500000 + ((year - 1990) * 10000)
        
        data.append({
            "year": int(year),
            "elective_surgery_waitlist_count": int(base_wait),
            "hospital_beds_per_1000": round(beds, 2),
            "school_enrollment_total": int(enroll)
        })
    return pd.DataFrame(data)

def generate_demographics():
    years = np.arange(1990, 2026)
    data = []
    
    for year in years:
        # Median Age (Aging population)
        # 1990: 32 -> 2024: 38
        age = 32.0 + ((year - 1990) * 0.17)
        
        # Migration vs Natural
        # Pre-2005: Natural dominant
        # Post-2005: Migration dominant
        if year < 2005:
            nat = 30000 + np.random.uniform(-2000, 2000)
            mig = 10000 + np.random.uniform(-5000, 5000)
        else:
            nat = 35000 + np.random.uniform(-2000, 2000)
            mig = 50000 + ((year - 2005) * 2000) # Ramping up
            
        if year in [2020, 2021]:
            mig = -5000 # COVID negative migration
        
        if year >= 2023:
            mig = 90000 # Post-COVID surge
            
        data.append({
            "year": int(year),
            "net_overseas_migration": int(mig),
            "median_age": round(age, 1),
            "natural_increase": int(nat)
        })
    return pd.DataFrame(data)

def ingest_final_layer():
    print("--- Starting Final Layer Ingest ---")
    engine = create_engine(DATABASE_URL)
    
    # 1. Environment
    env_df = generate_environment_data()
    with engine.connect() as conn:
        trans = conn.begin()
        conn.execute(text("DELETE FROM environmental_data"))
        trans.commit()
    env_df.to_sql('environmental_data', engine, if_exists='append', index=False)
    print("Environmental Data Ingested.")

    # 2. Health/Edu
    he_df = generate_health_edu_data()
    with engine.connect() as conn:
        trans = conn.begin()
        conn.execute(text("DELETE FROM health_education_stats"))
        trans.commit()
    he_df.to_sql('health_education_stats', engine, if_exists='append', index=False)
    print("Health & Education Stats Ingested.")

    # 3. Demographics
    demo_df = generate_demographics()
    with engine.connect() as conn:
        trans = conn.begin()
        conn.execute(text("DELETE FROM demographics_deep"))
        trans.commit()
    demo_df.to_sql('demographics_deep', engine, if_exists='append', index=False)
    print("Demographics Data Ingested.")
    
    # 4. Full Synthesis Check (Gap Fill)
    # Ensure every year from 2005-2025 exists in all single-year tables
    # (Tables that track 'year' as PK or Main Index)
    tables = [
        "economic_indicators", "state_budget", "social_indicators", 
        "industry_performance", "energy_metrics", "transport_stats",
        "environmental_data", "health_education_stats", "demographics_deep"
    ]
    
    print("\n--- Running Full Synthesis Check (2005-2025) ---")
    with engine.connect() as conn:
        for t in tables:
            # Check for missing years
            existing = pd.read_sql(f"SELECT year FROM {t} WHERE year BETWEEN 2005 AND 2025", conn)
            found_years = set(existing['year'].tolist())
            missing = [y for y in range(2005, 2026) if y not in found_years]
            
            if not missing:
                print(f"Table '{t}': Continuous.")
            else:
                print(f"Table '{t}': Missing years {missing}. Synthesis required (logic not fully implemented in this script but flagged).")
                # In this simulated run, our generators covered these ranges, so it should be continuous.

if __name__ == "__main__":
    ingest_final_layer()
