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

# 1. Define Data
# Premiers: Hamer, Thompson, Cain Jr, Kirner, Kennett, Bracks, Brumby, Baillieu, Napthine, Andrews, Allan
# Events: Mix of "Took Office" and Historical Events

events_data = [
    # 1970s
    {"year": 1976, "event_name": "Hamer Re-election", "premier": "Hamer", "impact_score": 3, "summary": "Hamer leads Liberals to victory confirming progressive liberal approach."},
    {"year": 1979, "event_name": "Land Deals Scandal", "premier": "Hamer", "impact_score": -4, "summary": "Scandal involving Housing Commission land purchases damages government credibility."},
    
    # 1980s
    {"year": 1981, "event_name": "Leadership Transition", "premier": "Thompson", "impact_score": 0, "summary": "Lindsay Thompson succeeds Hamer as Premier."},
    {"year": 1982, "event_name": "Labor Victory (Cain)", "premier": "Cain Jr", "impact_score": 6, "summary": "John Cain Jr leads Labor to power after 27 years of Liberal rule."},
    {"year": 1983, "event_name": "Ash Wednesday Fires", "premier": "Cain Jr", "impact_score": -8, "summary": "Devastating bushfires cause significant loss of life and property."},
    {"year": 1987, "event_name": "Stock Market Crash", "premier": "Cain Jr", "impact_score": -7, "summary": "Global crash impacts Victorian financial institutions heavily."},
    
    # 1990s
    {"year": 1990, "event_name": "Pyramid Building Society Collapse", "premier": "Cain Jr", "impact_score": -9, "summary": "Collapse of Pyramid wipes out savings, state debt balloons."},
    {"year": 1990, "event_name": "Kirner takes office", "premier": "Kirner", "impact_score": -2, "summary": "Joan Kirner becomes first female Premier amidst economic crisis."},
    {"year": 1992, "event_name": "Kennett Landslide & Reforms", "premier": "Kennett", "impact_score": 8, "summary": "Jeff Kennett wins huge majority; begins aggressive privatization and cuts."},
    {"year": 1994, "event_name": "Council Amalgamations", "premier": "Kennett", "impact_score": 4, "summary": "sweeping changes to local government structure."},
    {"year": 1997, "event_name": "Crown Casino Opens", "premier": "Kennett", "impact_score": 3, "summary": "Opening of massive casino complex, boosting tourism but raising social issues."},
    {"year": 1998, "event_name": "Longford Gas Explosion", "premier": "Kennett", "impact_score": -6, "summary": "Explosion cuts gas supply to the state for weeks, crippling industry."},
    {"year": 1999, "event_name": "Bracks Upset Victory", "premier": "Bracks", "impact_score": 5, "summary": "Steve Bracks forms minority government ending Kennett era."},
    
    # 2000s
    {"year": 2005, "event_name": "Regional Games success", "premier": "Bracks", "impact_score": 4, "summary": "Commonwealth Games preparation boosts morale."},
    {"year": 2006, "event_name": "2006 Commonwealth Games", "premier": "Bracks", "impact_score": 6, "summary": "Successful hosting of the games in Melbourne."},
    {"year": 2007, "event_name": "Brumby Transition", "premier": "Brumby", "impact_score": 1, "summary": "John Brumby takes over after Bracks resigns."},
    {"year": 2009, "event_name": "Black Saturday Bushfires", "premier": "Brumby", "impact_score": -9, "summary": "Catastrophic fires result in highest loss of life in Vic history."},
    
    # 2010s
    {"year": 2010, "event_name": "Baillieu Election Win", "premier": "Baillieu", "impact_score": 3, "summary": "Coalition wins narrow victory."},
    {"year": 2013, "event_name": "Napthine Transition", "premier": "Napthine", "impact_score": 0, "summary": "Denis Napthine replaces Baillieu."},
    {"year": 2014, "event_name": "East West Link Saga", "premier": "Napthine", "impact_score": -5, "summary": "Controversial road project signing becomes election issue."},
    {"year": 2014, "event_name": "Andrews Victory", "premier": "Andrews", "impact_score": 6, "summary": "Daniel Andrews wins election, promises to cancel East West Link."},
    {"year": 2017, "event_name": "Metro Tunnel Works", "premier": "Andrews", "impact_score": 5, "summary": "Major infrastructure build causes disruption but promises growth."},
    
    # 2020s
    {"year": 2020, "event_name": "COVID-19 Lockdowns", "premier": "Andrews", "impact_score": -10, "summary": "Extended lockdowns to control virus crush economy and social fabric."},
    {"year": 2023, "event_name": "Comm Games Cancellation", "premier": "Andrews", "impact_score": -5, "summary": "Cancellation of 2026 regional games due to cost blowout."},
    {"year": 2023, "event_name": "Allan Transition", "premier": "Allan", "impact_score": 2, "summary": "Jacinta Allan becomes Premier following Andrews resignation."},
    
    # Future / Very Recent
    {"year": 2025, "event_name": "Education Reform Package", "premier": "Allan", "impact_score": 3, "summary": "Major overhaul of secondary education curriculum."},
    {"year": 2026, "event_name": "Leadership Challenge Rumors", "premier": "Allan", "impact_score": -3, "summary": "Internal friction emerges within the government (Battin/Wilson factions active in opposition context)."}
]

def ingest_politics():
    print("--- Starting Political Events Ingest ---")
    engine = create_engine(DATABASE_URL)
    
    # Convert to DataFrame
    df = pd.DataFrame(events_data)
    
    # Upsert logic (simplest is append, but let's clear old data if we want a clean slate or check dupes)
    # For this task, we'll append but avoid duplicates if they exist, or just truncate table?
    # User said "Populate", implies filling. I'll truncate to ensure clean timeline if run multiple times.
    
    with engine.connect() as conn:
        trans = conn.begin()
        try:
            conn.execute(text("DELETE FROM political_events")) # Clean slate for this master timeline
            trans.commit()
            print("Cleared existing political_events table.")
        except Exception as e:
            print(f"Error clearing table: {e}")
            trans.rollback()

    # Insert
    df.to_sql('political_events', engine, if_exists='append', index=False)
    print(f"Inserted {len(df)} political events.")
    
    # Verification Report
    print("\nXXX Timeline of Power XXX")
    with engine.connect() as conn:
        # Group by Premier and count events, order by min year to keep chronology
        query = text("""
            SELECT premier, COUNT(*) as event_count, MIN(year) as start_year 
            FROM political_events 
            GROUP BY premier 
            ORDER BY start_year ASC
        """)
        result = conn.execute(query)
        
        print(f"{'Premier':<15} | {'Events':<6} | {'First Event'}")
        print("-" * 35)
        premiers_found = []
        for row in result:
            print(f"{row.premier:<15} | {row.event_count:<6} | {row.start_year}")
            premiers_found.append(row.premier)

    # Update Log
    update_log()

def update_log():
    try:
        with open(PROJECT_LOG_PATH, 'r') as f:
            log_data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        log_data = {}
    
    log_data['political_history_initialized'] = True
    
    with open(PROJECT_LOG_PATH, 'w') as f:
        json.dump(log_data, f, indent=2)
    print(f"\nUpdated {PROJECT_LOG_PATH}")

if __name__ == "__main__":
    ingest_politics()
