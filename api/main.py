from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine, text
import pandas as pd
import geopandas as gpd
import json
import os

app = FastAPI()

# Configuration
BASE_DIR = os.getcwd()
DB_PATH = os.path.join(BASE_DIR, 'data', 'processed', 'victoria_sim.db')
DATABASE_URL = f"sqlite:///{DB_PATH}"
GEO_FILE = os.path.join(BASE_DIR, 'data', 'geo', 'vic_lgas_2026.json')

# Database Initialization
engine = create_engine(DATABASE_URL)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # In production, lock this down
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/api/v1/health")
def health_check():
    return {"status": "online"}

@app.get("/api/v1/stats/{year}")
def get_state_stats(year: int):
    # Mocking aggregated stats for the header cards
    with engine.connect() as conn:
        # Example: Sum of GSP? Or just specific indicators?
        # Let's pull state-wide temporal stats if available, or aggregating LGA stats
        # For simplicity, returning mock data structure expected by frontend, but populated from DB if possible
        
        # Try to find State-wide 'GSP', 'Debt', 'Population'
        try:
            res = conn.execute(text("""
                SELECT category, value FROM temporal_stats 
                WHERE year = :y AND region_type = 'State'
            """), {"y": year}).fetchall()
            data_map = {row[0]: row[1] for row in res}
            
            # Map to frontend keys
            return {
                "gdp": data_map.get("GSP (Gross State Product)", 400 + (year-2000)*5), 
                "population": data_map.get("Population", 6.5 + (year-2000)*0.1),
                "debt": data_map.get("Household Debt-to-Income", 100), # Not exactly state debt but close metric
                "gini": 0.33
            }
        except Exception as e:
            # Fallback
            return {
                "gdp": 420.0,
                "population": 6.8,
                "debt": 120.0,
                "gini": 0.35
            }

@app.get("/api/v1/map/{year}/{metric_id}")
def get_map_layer(year: int, metric_id: int):
    """
    Returns a GeoJSON FeatureCollection with 'metric_value' and 'normalized_score'.
    metric_id refers to category_id in temporal_stats.
    """
    try:
        # 1. Load GeoJSON (Spatial Layer)
        if not os.path.exists(GEO_FILE):
             raise HTTPException(status_code=404, detail="GeoJSON file not found")
        
        gdf = gpd.read_file(GEO_FILE)
        
        # 2. Get Data for Year (Temporal Layer)
        query = text("""
            SELECT lga_code, value 
            FROM temporal_stats 
            WHERE year = :y AND category_id = :c AND lga_code IS NOT NULL
        """)
        
        with engine.connect() as conn:
            df_year = pd.read_sql(query, conn, params={"y": year, "c": metric_id})
            
            # 3. Get Global Min/Max for Scaling
            # We want the min/max for this metric across ALL years to ensure stable coloring
            query_global = text("""
                SELECT MIN(value) as min_val, MAX(value) as max_val
                FROM temporal_stats
                WHERE category_id = :c AND lga_code IS NOT NULL
            """)
            stats = conn.execute(query_global, {"c": metric_id}).fetchone()
            min_val = stats[0] if stats[0] is not None else 0
            max_val = stats[1] if stats[1] is not None else 1
            
            if max_val == min_val: max_val = min_val + 1 # Prevent divide by zero

        # 4. Join
        # Ensure LGA_CODE types match
        if 'LGA_CODE' not in gdf.columns:
             # Fallback if mock data lacks it, provided mock has it
             raise HTTPException(status_code=500, detail="GeoJSON missing LGA_CODE property")
        
        gdf['LGA_CODE'] = gdf['LGA_CODE'].astype(int)
        
        # Merge
        # Left join to keep all geometries
        merged = gdf.merge(df_year, left_on='LGA_CODE', right_on='lga_code', how='left')
        
        # 5. Normalize
        merged['metric_value'] = merged['value'].fillna(0)
        merged['normalized_score'] = (merged['metric_value'] - min_val) / (max_val - min_val)
        
        # Handle NaN from join
        merged['normalized_score'] = merged['normalized_score'].fillna(0).clip(0, 1)

        # 6. Convert to GeoJSON
        # We need to return a Python dict that FastAPI can serialize
        return json.loads(merged.to_json())

    except Exception as e:
        print(f"Error serving map: {e}")
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
