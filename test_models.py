# test_models.py

from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from config.settings import DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME
from models import Base, State, District, Block, Panchayat

# Setup the database engine
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DATABASE_URL)

# Create a configured "Session" class
Session = sessionmaker(bind=engine)

# Create a session
session = Session()

try:
    # Count records in each table
    states_count = session.query(State).count()
    districts_count = session.query(District).count()
    blocks_count = session.query(Block).count()
    panchayats_count = session.query(Panchayat).count()

    # Print the counts
    print(f"Number of records in 'states' table: {states_count}")
    print(f"Number of records in 'districts' table: {districts_count}")
    print(f"Number of records in 'blocks' table: {blocks_count}")
    print(f"Number of records in 'panchayats' table: {panchayats_count}")

except Exception as e:
    print(f"Error querying tables: {e}")

finally:
    session.close()