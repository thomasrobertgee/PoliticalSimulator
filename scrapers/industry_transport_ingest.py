import pandas as pd
from sqlalchemy import create_engine, text
import numpy as np
import os

# Configuration
BASE_DIR = os.getcwd()
DB_PATH = os.path.join(BASE_DIR, 'data', 'processed', 'victoria_sim.db')
DATABASE_URL = f"sqlite:///{DB_PATH}"

def generate_industry_data():
    years = np.arange(2000, 2026)
    sectors = ["Manufacturing", "Agriculture", "Health", "Finance", "Technology"]
    data = []
    
    for year in years:
        progress = (year - 2000) / 25.0
        
        # Mfg: Decline. Start 30B -> 25B
        mfg = 30.0 - (5.0 * progress)
        
        # Ag: Stable. 10B -> 12B
        ag = 10.0 + (2.0 * progress)
        
        # Health: Boom. 15B -> 40B
        health = 15.0 + (25.0 * progress)
        
        # Finance: Strong Growth. 25B -> 55B
        fin = 25.0 + (30.0 * progress)
        
        # Tech: Explosion. 5B -> 25B
        tech = 5.0 + (20.0 * progress)
        
        vals = {"Manufacturing": mfg, "Agriculture": ag, "Health": health, "Finance": fin, "Technology": tech}
        
        for k, v in vals.items():
            data.append({
                "year": int(year),
                "sector_name": k,
                "gva_billions": round(v, 1)
            })
    return pd.DataFrame(data)

def generate_energy_data():
    years = np.arange(2000, 2026)
    data = []
    
    for year in years:
        # Renewables
        if year < 2010:
            ren = 5.0 + ((year - 2000) * 0.5) # Slow start
        elif year < 2018:
             ren = 10.0 + ((year - 2010) * 1.5) # Picking up
        else:
             ren = 22.0 + ((year - 2018) * 3.5) # Acceleration (VRET)
        if ren > 90: ren = 90 # Cap
        
        # Coal MW (Declining)
        coal = 6000 - ((year - 2000) * 120) 
        if year > 2017: coal -= 1000 # (Hazelwood effect approx)
        if coal < 2000: coal = 2000
        
        reliability = 99.9
        
        data.append({
            "year": int(year),
            "renewable_percentage": round(ren, 1),
            "coal_generation_mw": int(coal),
            "reliability_index": reliability
        })
    return pd.DataFrame(data)

def generate_transport_data():
    years = np.arange(2000, 2026)
    data = []
    
    for year in years:
        # Trends
        if year < 2020:
             # Growth
             metro = 150 + ((year-2000) * 5)
             vline = 10 + ((year-2000) * 0.6)
             tram = 120 + ((year-2000) * 4)
        elif year in [2020, 2021]:
             # Crash
             metro = 80
             vline = 8
             tram = 60
        elif year == 2022:
             # Recovery
             metro = 160
             vline = 15
             tram = 140
        else:
             # 2023+ (Fare Cap surge for Vline)
             metro = 210 + ((year-2023)*10)
             vline = 22 + ((year-2023)*2) # Big jump from fare cap
             tram = 190 + ((year-2023)*5)
        
        data.append({
            "year": int(year),
            "metro_train_patronage_millions": round(metro, 1),
            "v_line_patronage_millions": round(vline, 1),
            "tram_patronage_millions": round(tram, 1)
        })
    return pd.DataFrame(data)

def ingest_extensions():
    print("--- Starting Industry & Transport Ingest ---")
    engine = create_engine(DATABASE_URL)
    
    # 1. Industry
    ind_df = generate_industry_data()
    with engine.connect() as conn:
        trans = conn.begin()
        conn.execute(text("DELETE FROM industry_performance"))
        trans.commit()
    ind_df.to_sql('industry_performance', engine, if_exists='append', index=False)
    print("Industry Data Ingested.")
    
    # 2. Energy
    nrg_df = generate_energy_data()
    with engine.connect() as conn:
        trans = conn.begin()
        conn.execute(text("DELETE FROM energy_metrics"))
        trans.commit()
    nrg_df.to_sql('energy_metrics', engine, if_exists='append', index=False)
    print("Energy Metrics Ingested.")

    # 3. Transport
    trans_df = generate_transport_data()
    with engine.connect() as conn:
        trans = conn.begin()
        conn.execute(text("DELETE FROM transport_stats"))
        trans.commit()
    trans_df.to_sql('transport_stats', engine, if_exists='append', index=False)
    print("Transport Stats Ingested.")

if __name__ == "__main__":
    ingest_extensions()
