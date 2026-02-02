import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
import os

# Configuration
BASE_DIR = os.getcwd()
DB_PATH = os.path.join(BASE_DIR, 'data', 'processed', 'victoria_sim.db')
DATABASE_URL = f"sqlite:///{DB_PATH}"

# List of 50 Categories (Selected subset for simulation relevance)
CATEGORIES = [
    # Economy
    "CPI (Melbourne)", "Retail Trade Index", "Household Debt-to-Income", "Bankruptcy Count",
    # Politics
    "Primary Vote Labor", "Primary Vote Liberal", "Legislative Acts Passed",
    # Infrastructure
    "AADT Major Arteries", "Public Transport Mode Share", "Road Mode Share",
    # Social
    "Homelessness Count", "Family Violence Incidents", "Year 12 Completion Rate",
    # Environment
    "PM2.5 Average", "Carbon Emissions (Mt)", "Rainfall Variability Index"
]

def generate_temporal_data():
    years = np.arange(1976, 2027) # 1976 to 2026
    data = []
    
    for year in years:
        progress = (year - 1976) / 50.0
        
        # 1. CPI (Base 100 in 2012 approx)
        # 1976: ~20. 2026: ~140
        cpi = 20.0 + (120 * progress) + np.random.normal(0, 1)
        data.append({"year": year, "category": "CPI (Melbourne)", "value": round(cpi, 1), "region_type": "State", "region_name": "Victoria"})
        
        # 2. Household Debt Ratio
        # 1976: 40%. 2026: 190%
        debt = 40.0 + (150.0 * (progress**1.5)) # Accelerating curve
        data.append({"year": year, "category": "Household Debt-to-Income", "value": round(debt, 1), "region_type": "State", "region_name": "Victoria"})
        
        # 3. Bankruptcy (Cyclical)
        bankrupt = 2000 + (1000 * np.sin(year/5)) + (progress * 500)
        if year in [1991, 1992, 2020]: bankrupt += 1500
        data.append({"year": year, "category": "Bankruptcy Count", "value": int(bankrupt), "region_type": "State", "region_name": "Victoria"})
        
        # 4. Politics (Vote Share Volatility)
        # Labor 45% avg, Lib 42% avg
        lab = 45 + np.random.uniform(-8, 8)
        lib = 42 + np.random.uniform(-8, 8)
        data.append({"year": year, "category": "Primary Vote Labor", "value": round(lab, 1), "region_type": "State", "region_name": "Victoria"})
        data.append({"year": year, "category": "Primary Vote Liberal", "value": round(lib, 1), "region_type": "State", "region_name": "Victoria"})
        
        # 5. Infrastructure (Mode Share)
        # PT: 1976 (Highish) -> 1990 (Low) -> 2026 (High)
        if year < 1990: pt = 15 - ((year-1976)*0.3)
        else: pt = 10 + ((year-1990)*0.2)
        data.append({"year": year, "category": "Public Transport Mode Share", "value": round(pt, 1), "region_type": "State", "region_name": "Victoria"})
        
        # 6. Social (Homelessness)
        # Rising
        home = 10000 + (20000 * progress)
        data.append({"year": year, "category": "Homelessness Count", "value": int(home), "region_type": "State", "region_name": "Victoria"})
        
        # 7. Environment (Emissions)
        # Peak 2005, then drop
        if year < 2005: emit = 80 + ((year-1976)*1)
        else: emit = 110 - ((year-2005)*2.5) # Aggressive drop
        data.append({"year": year, "category": "Carbon Emissions (Mt)", "value": round(emit, 1), "region_type": "State", "region_name": "Victoria"})
        
        # 8. Rainfall Var (Indices)
        rain_var = 1.0 + (progress * 0.5) # Becoing more variable
        data.append({"year": year, "category": "Rainfall Variability Index", "value": round(rain_var, 2), "region_type": "State", "region_name": "Victoria"})

    return pd.DataFrame(data)

def ingest_temporal_stats():
    print("--- Starting Temporal Stats Harvest ---")
    engine = create_engine(DATABASE_URL)
    
    df = generate_temporal_data()
    
    with engine.connect() as conn:
        trans = conn.begin()
        conn.execute(text("DELETE FROM temporal_stats"))
        trans.commit()
    
    df.to_sql('temporal_stats', engine, if_exists='append', index=False)
    print(f"Harvested {len(df)} temporal data points (1976-2026).")
    
    # Verification
    print("\nXXX Temporal Archive Report XXX")
    print(f"{'Category':<30} | {'1976 Val':<10} | {'2026 Val':<10}")
    print("-" * 55)
    
    cats = ["CPI (Melbourne)", "Household Debt-to-Income", "Carbon Emissions (Mt)", "Public Transport Mode Share"]
    for c in cats:
        v76 = df[(df['year'] == 1976) & (df['category'] == c)]['value'].values[0]
        v26 = df[(df['year'] == 2026) & (df['category'] == c)]['value'].values[0]
        print(f"{c:<30} | {v76:<10} | {v26:<10}")

if __name__ == "__main__":
    ingest_temporal_stats()
