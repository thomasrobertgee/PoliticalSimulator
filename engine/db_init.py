import os
from sqlalchemy import create_engine, Column, Integer, String, Float, Text
from sqlalchemy.orm import declarative_base, sessionmaker

# Define database path
DB_DIR = os.path.join(os.getcwd(), 'data', 'processed')
os.makedirs(DB_DIR, exist_ok=True)
DB_PATH = os.path.join(DB_DIR, 'victoria_sim.db')
DATABASE_URL = f"sqlite:///{DB_PATH}"

Base = declarative_base()

# --- Existing Tables ---
class EconomicIndicators(Base):
    __tablename__ = 'economic_indicators'
    year = Column(Integer, primary_key=True)
    gsp_billions = Column(Float)
    unemployment_rate = Column(Float)
    state_debt_billions = Column(Float)
    population_millions = Column(Float)

class PoliticalEvents(Base):
    __tablename__ = 'political_events'
    id = Column(Integer, primary_key=True, autoincrement=True)
    year = Column(Integer)
    event_name = Column(String)
    premier = Column(String)
    impact_score = Column(Integer)
    summary = Column(Text)

class LGAStats(Base):
    __tablename__ = 'lga_stats'
    id = Column(Integer, primary_key=True, autoincrement=True)
    lga_name = Column(String)
    year = Column(Integer)
    population = Column(Integer)
    median_house_price = Column(Float)
    political_lean = Column(String)

class StateBudget(Base):
    __tablename__ = 'state_budget'
    year = Column(Integer, primary_key=True)
    total_revenue = Column(Float)
    total_expenditure = Column(Float)
    infrastructure_spend = Column(Float)

class InfrastructureProjects(Base):
    __tablename__ = 'infrastructure_projects'
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String)
    start_year = Column(Integer)
    completion_year = Column(Integer)
    budget_billions = Column(Float)
    region_impacted = Column(String)

class SocialIndicators(Base):
    __tablename__ = 'social_indicators'
    year = Column(Integer, primary_key=True)
    crime_rate_per_100k = Column(Float)
    education_rank = Column(Integer)
    health_satisfaction_score = Column(Float)

class IndustryPerformance(Base):
    __tablename__ = 'industry_performance'
    id = Column(Integer, primary_key=True, autoincrement=True)
    year = Column(Integer)
    sector_name = Column(String)
    gva_billions = Column(Float)

class EnergyMetrics(Base):
    __tablename__ = 'energy_metrics'
    year = Column(Integer, primary_key=True)
    renewable_percentage = Column(Float)
    coal_generation_mw = Column(Float)
    reliability_index = Column(Float)

class TransportStats(Base):
    __tablename__ = 'transport_stats'
    year = Column(Integer, primary_key=True)
    metro_train_patronage_millions = Column(Float)
    v_line_patronage_millions = Column(Float)
    tram_patronage_millions = Column(Float)

class EnvironmentalData(Base):
    __tablename__ = 'environmental_data'
    year = Column(Integer, primary_key=True)
    avg_rainfall_mm = Column(Float)
    melbourne_water_storage_pct = Column(Float)
    bushfire_area_hectares = Column(Float)

class HealthEducationStats(Base):
    __tablename__ = 'health_education_stats'
    year = Column(Integer, primary_key=True)
    elective_surgery_waitlist_count = Column(Integer)
    hospital_beds_per_1000 = Column(Float)
    school_enrollment_total = Column(Integer)

class DemographicsDeep(Base):
    __tablename__ = 'demographics_deep'
    year = Column(Integer, primary_key=True)
    net_overseas_migration = Column(Integer)
    median_age = Column(Float)
    natural_increase = Column(Integer)

class ElectionResults(Base):
    __tablename__ = 'election_results'
    year = Column(Integer, primary_key=True)
    winning_party = Column(String)
    primary_vote_pct = Column(Float)
    seat_count = Column(Integer)
    opposition_leader = Column(String)

class MacroAdjusters(Base):
    __tablename__ = 'macro_adjusters'
    year = Column(Integer, primary_key=True)
    cpi_inflation_rate = Column(Float)
    wage_price_index_growth = Column(Float)
    interest_rate_average = Column(Float)

class DetailedSpending(Base):
    __tablename__ = 'detailed_spending'
    year = Column(Integer, primary_key=True)
    health_spend_pct = Column(Float)
    education_spend_pct = Column(Float)
    police_justice_spend_pct = Column(Float)
    infrastructure_spend_pct = Column(Float)

class LandUseZones(Base):
    __tablename__ = 'land_use_zones'
    id = Column(Integer, primary_key=True, autoincrement=True)
    lga_name = Column(String)
    zone_type = Column(String)
    percentage_coverage = Column(Float)

class HousingDiversity(Base):
    __tablename__ = 'housing_diversity'
    id = Column(Integer, primary_key=True, autoincrement=True)
    year = Column(Integer)
    lga_name = Column(String)
    standalone_house_pct = Column(Float)
    apartment_pct = Column(Float)
    townhouse_pct = Column(Float)

class EmploymentHubs(Base):
    __tablename__ = 'employment_hubs'
    id = Column(Integer, primary_key=True, autoincrement=True)
    hub_name = Column(String)
    lga_name = Column(String)
    estimated_jobs = Column(Integer)
    primary_industry = Column(String)

# --- New Table (Step 171) ---
class TemporalStats(Base):
    __tablename__ = 'temporal_stats'
    id = Column(Integer, primary_key=True, autoincrement=True)
    year = Column(Integer)
    category = Column(String) # e.g., "Household Debt Index", "Homelessness Count"
    value = Column(Float)
    region_type = Column(String) # "State" or "LGA"
    region_name = Column(String) # "Victoria" or LGA Name

def init_db():
    print(f"Update: Creating/Refreshing database tables at {DB_PATH}...")
    engine = create_engine(DATABASE_URL)
    Base.metadata.create_all(engine)
    return engine

def seed_db(engine):
    Session = sessionmaker(bind=engine)
    session = Session()
    existing = session.query(EconomicIndicators).filter_by(year=2024).first()
    if not existing:
        print("Seeding test data for 2024...")
        test_data = EconomicIndicators(
            year=2024,
            gsp_billions=580.0,
            unemployment_rate=4.0,
            state_debt_billions=150.0,
            population_millions=6.9
        )
        session.add(test_data)
        session.commit()
    session.close()

if __name__ == "__main__":
    engine = init_db()
    seed_db(engine)
