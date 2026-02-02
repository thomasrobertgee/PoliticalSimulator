import pandas as pd
from sqlalchemy import create_engine, text
import numpy as np
import os

# Configuration
BASE_DIR = os.getcwd()
DB_PATH = os.path.join(BASE_DIR, 'data', 'processed', 'victoria_sim.db')
DATABASE_URL = f"sqlite:///{DB_PATH}"

def generate_election_data():
    # Only election years
    # 1976 Hamer (already in power), 1979 Hamer, 1982 Cain, 1985 Cain, 1988 Cain, 1992 Kennett, 1996 Kennett, 1999 Bracks, 2002 Bracks, 2006 Bracks, 2010 Baillieu, 2014 Andrews, 2018 Andrews, 2022 Andrews. 
    elections = [
        {"year": 1976, "party": "Liberal", "prim": 45.9, "seat": 52, "opp": "Holdming"},
        {"year": 1979, "party": "Liberal", "prim": 41.4, "seat": 41, "opp": "Wilkes"},
        {"year": 1982, "party": "Labor", "prim": 50.0, "seat": 49, "opp": "Thompson"},
        {"year": 1985, "party": "Labor", "prim": 49.0, "seat": 47, "opp": "Kennett"},
        {"year": 1988, "party": "Labor", "prim": 46.0, "seat": 46, "opp": "Kennett"},
        {"year": 1992, "party": "Liberal/Nat", "prim": 52.0, "seat": 58, "opp": "Kirner"},
        {"year": 1996, "party": "Liberal/Nat", "prim": 50.7, "seat": 58, "opp": "Brumby"},
        {"year": 1999, "party": "Labor (Min)", "prim": 45.0, "seat": 42, "opp": "Kennett"},
        {"year": 2002, "party": "Labor", "prim": 49.0, "seat": 62, "opp": "Doyle"},
        {"year": 2006, "party": "Labor", "prim": 43.0, "seat": 55, "opp": "Baillieu"},
        {"year": 2010, "party": "Liberal/Nat", "prim": 44.8, "seat": 45, "opp": "Brumby"},
        {"year": 2014, "party": "Labor", "prim": 38.0, "seat": 47, "opp": "Napthine"},
        {"year": 2018, "party": "Labor", "prim": 42.9, "seat": 55, "opp": "Guy"},
        {"year": 2022, "party": "Labor", "prim": 37.0, "seat": 56, "opp": "Guy"}
    ]
    
    data = []
    for e in elections:
        data.append({
            "year": e["year"],
            "winning_party": e["party"],
            "primary_vote_pct": e["prim"],
            "seat_count": e["seat"],
            "opposition_leader": e["opp"]
        })
    return pd.DataFrame(data)

def generate_macro_data():
    years = np.arange(1990, 2026)
    data = []
    
    for year in years:
        # Inflation (RBA target 2-3%, but varied)
        cpi = 2.5 + np.random.uniform(-1.0, 1.0)
        if year < 1995: cpi = 4.0 + np.random.uniform(0, 2) # Early 90s residual
        if year in [2022, 2023]: cpi = 6.0 + np.random.uniform(0, 1.5) # Post-COVID spike
        
        # WPI (Usually around 3-4%, lower lately)
        wpi = 3.5
        if year > 2013: wpi = 2.2 # Stagnation
        if year > 2021: wpi = 3.8 # Recent catchup
        
        # Interest Rate (Cash Rate Avg)
        ir = 5.0
        if year < 1996: ir = 10.0 + np.random.uniform(0, 4) # 17% recession era
        elif year < 2008: ir = 5.5
        elif year < 2020: ir = 2.0
        elif year < 2022: ir = 0.1
        else: ir = 4.35
        
        data.append({
            "year": int(year),
            "cpi_inflation_rate": round(cpi, 1),
            "wage_price_index_growth": round(wpi, 1),
            "interest_rate_average": round(ir, 2)
        })
    return pd.DataFrame(data)

def generate_detailed_spending():
    years = np.arange(1990, 2026)
    data = []
    
    for year in years:
        # Spending Shifts
        # Health: Always rising (25% -> 32%)
        health = 25.0 + ((year - 1990) * 0.2)
        
        # Education: Stable (22% -> 24%)
        edu = 22.0 + ((year - 1990) * 0.05)
        
        # Infra: Highly political.
        # Kennett (90s): Low/Privatized (5%)
        # Bracks (00s): Moderate (8%)
        # Andrews (15+): Massive (15%+)
        infra = 8.0
        if 1992 <= year <= 1999: infra = 4.0
        if 2015 <= year: infra = 16.0
        
        # Police/Justice (Stable ~10%)
        police = 10.0
        
        # Normalization (roughly, we just want trends)
        
        data.append({
            "year": int(year),
            "health_spend_pct": round(health, 1),
            "education_spend_pct": round(edu, 1),
            "police_justice_spend_pct": round(police, 1),
            "infrastructure_spend_pct": round(infra, 1)
        })
    return pd.DataFrame(data)

def ingest_mandate_layer():
    print("--- Starting Mandate Layer Ingest ---")
    engine = create_engine(DATABASE_URL)
    
    # 1. Elections
    el_df = generate_election_data()
    with engine.connect() as conn:
        trans = conn.begin()
        conn.execute(text("DELETE FROM election_results"))
        trans.commit()
    el_df.to_sql('election_results', engine, if_exists='append', index=False)
    print("Election Results Ingested.")

    # 2. Macro
    mac_df = generate_macro_data()
    with engine.connect() as conn:
        trans = conn.begin()
        conn.execute(text("DELETE FROM macro_adjusters"))
        trans.commit()
    mac_df.to_sql('macro_adjusters', engine, if_exists='append', index=False)
    print("Macro Adjusters Ingested.")

    # 3. Spending
    spd_df = generate_detailed_spending()
    with engine.connect() as conn:
        trans = conn.begin()
        conn.execute(text("DELETE FROM detailed_spending"))
        trans.commit()
    spd_df.to_sql('detailed_spending', engine, if_exists='append', index=False)
    print("Detailed Spending Ingested.")

if __name__ == "__main__":
    ingest_mandate_layer()
