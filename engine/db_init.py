import os
from sqlalchemy import create_engine, Column, Integer, String, Float, Text
from sqlalchemy.orm import declarative_base, sessionmaker

# Define database path
DB_DIR = os.path.join(os.getcwd(), 'data', 'processed')
os.makedirs(DB_DIR, exist_ok=True)
DB_PATH = os.path.join(DB_DIR, 'victoria_sim.db')
DATABASE_URL = f"sqlite:///{DB_PATH}"

Base = declarative_base()

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
    impact_score = Column(Integer) # -10 to +10
    summary = Column(Text)

class LGAStats(Base):
    __tablename__ = 'lga_stats'
    id = Column(Integer, primary_key=True, autoincrement=True)
    lga_name = Column(String)
    year = Column(Integer)
    population = Column(Integer)
    median_house_price = Column(Float)

def init_db():
    print(f"Creating database tables at {DB_PATH}...")
    engine = create_engine(DATABASE_URL)
    Base.metadata.create_all(engine)
    return engine

def seed_db(engine):
    Session = sessionmaker(bind=engine)
    session = Session()

    # Check if 2024 data already exists to prevent duplicates on re-run
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
        print("Seeding complete.")
    else:
        print("2024 test data already exists.")
    session.close()

def verify_db(engine):
    Session = sessionmaker(bind=engine)
    session = Session()
    data = session.query(EconomicIndicators).filter_by(year=2024).first()
    if data:
        print("\n--- Verification Result ---")
        print(f"Year: {data.year}")
        print(f"GSP: ${data.gsp_billions}B")
        print(f"Unemployment: {data.unemployment_rate}%")
        print(f"State Debt: ${data.state_debt_billions}B")
        print(f"Population: {data.population_millions}M")
        print("---------------------------\nDatabase verification successful.")
    else:
        print("Verification FAILED: Test row not found.")
    session.close()

if __name__ == "__main__":
    engine = init_db()
    seed_db(engine)
    verify_db(engine)
