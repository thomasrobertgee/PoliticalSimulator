from sqlalchemy import create_engine, text
import pandas as pd
import os
import json

class VictoriaState:
    def __init__(self):
        # Database connection
        self.base_dir = os.getcwd()
        self.db_path = os.path.join(self.base_dir, 'data', 'processed', 'victoria_sim.db')
        self.db_url = f"sqlite:///{self.db_path}"
        self.engine = create_engine(self.db_url)
    
    # ... (Previous get_state methods implied)

    def get_regional_friction(self, lga_name: str, year: int = 2024):
        """
        Calculates Commute Stress Score based on Jobs/Workers ratio.
        """
        with self.engine.connect() as conn:
            # 1. Workers (Resident Population * ~0.6 participation)
            pop_res = conn.execute(text("SELECT population FROM lga_stats WHERE lga_name = :l AND year = :y"), 
                                   {"l": lga_name, "y": year}).scalar()
            
            if not pop_res: return "N/A"
            workers = pop_res * 0.6
            
            # 2. Local Jobs (Sum of Hubs + Base Employment)
            # Hubs
            hub_jobs = conn.execute(text("SELECT SUM(estimated_jobs) FROM employment_hubs WHERE lga_name = :l"), 
                                    {"l": lga_name}).scalar() or 0
            
            # Base Jobs (Retail/Service ~20% of pop)
            base_jobs = pop_res * 0.2
            
            total_local_jobs = hub_jobs + base_jobs
            
            # 3. Ratio
            ratio = total_local_jobs / workers if workers > 0 else 0
            
            # Friction/Stress (Low ratio = High Out-Commute = High Stress)
            # Ideal Ratio ~ 1.0
            
            stress_score = max(0, 100 - (ratio * 100))
            
            return {
                "residents": int(pop_res),
                "workers_est": int(workers),
                "local_jobs": int(total_local_jobs),
                "jobs_ratio": round(ratio, 2),
                "commute_stress_score": round(stress_score, 1)
            }

def verify_spatial():
    vic = VictoriaState()
    lga = "Hobsons Bay"
    
    print(f"\nXXX Local Stress Profile: {lga} (2024) XXX")
    
    # 1. Zoning
    with vic.engine.connect() as conn:
        zones = conn.execute(text("SELECT zone_type, percentage_coverage FROM land_use_zones WHERE lga_name = :l"), 
                             {"l": lga}).mappings().all()
        print("--- Zoning Split ---")
        for z in zones:
            print(f"{z['zone_type']}: {z['percentage_coverage']}%")
            
    # 2. Friction
    fric = vic.get_regional_friction(lga, 2024)
    print("\n--- Commute Dynamics ---")
    print(f"Workers vs Jobs: {fric['workers_est']} vs {fric['local_jobs']}")
    print(f"Self-Containment Ratio: {fric['jobs_ratio']}")
    print(f"Commute Stress Score: {fric['commute_stress_score']}/100")
    
    # 3. Diversity Growth
    print("\n--- Housing Diversity Check ---")
    with vic.engine.connect() as conn:
        d11 = conn.execute(text("SELECT apartment_pct FROM housing_diversity WHERE lga_name=:l AND year=2011"), {"l": lga}).scalar()
        d24 = conn.execute(text("SELECT apartment_pct FROM housing_diversity WHERE lga_name=:l AND year=2024"), {"l": lga}).scalar()
        print(f"Apartment % (2011): {d11}%")
        print(f"Apartment % (2024): {d24}%")

if __name__ == "__main__":
    verify_spatial()
