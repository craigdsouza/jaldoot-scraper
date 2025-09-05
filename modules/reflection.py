from sqlalchemy.ext.automap import automap_base
from sqlalchemy import create_engine, MetaData, Table
from config.settings import DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Create engine but don't connect immediately
engine = create_engine(DATABASE_URL)

# Try to connect and reflect tables, but handle gracefully if tables don't exist
reflected_tables = {}

try:
    # Test connection
    with engine.connect() as connection:
        print("Connected to the database.")

        # Create a metadata object: metadata
        metadata = MetaData()
        metadata.reflect(engine)
        print("Metadata object created successfully.")

        # List of tables to reflect
        tables_to_reflect = ['states', 'districts', 'blocks', 'panchayats']

        for table_name in tables_to_reflect:
            try:
                table = Table(table_name, metadata, autoload_with=engine)
                reflected_tables[table_name] = table
                print(f"Table '{table_name}' reflected successfully.")
            except Exception as e:
                print(f"Table '{table_name}' not found (this is normal if not created yet): {e}")
                pass
except Exception as e:
    print(f"Database connection failed (this is normal if database setup is incomplete): {e}")
    print("The scraper will create tables automatically when run.")
    # Create dummy tables for now
    from sqlalchemy import Column, Integer, String, Text, Float
    Base = automap_base()

    class DummyState(Base):
        __tablename__ = 'states'
        id = Column(Integer, primary_key=True)
        name = Column(String(255))

    class DummyDistrict(Base):
        __tablename__ = 'districts'
        id = Column(Integer, primary_key=True)
        name = Column(String(255))

    class DummyBlock(Base):
        __tablename__ = 'blocks'
        id = Column(Integer, primary_key=True)
        name = Column(String(255))

    class DummyPanchayat(Base):
        __tablename__ = 'panchayats'
        id = Column(Integer, primary_key=True)
        name = Column(String(255))

    reflected_tables = {
        'states': DummyState.__table__,
        'districts': DummyDistrict.__table__,
        'blocks': DummyBlock.__table__,
        'panchayats': DummyPanchayat.__table__
    }

# Export reflected tables
__all__ = ['reflected_tables']


# # Reflect census table from the engine: census
# states_table = Table('states',metadata, autoload_with=engine) # type: sqlalchemy.sql.schema.Table
# print("Table reflected successfully.")

# # Print all column names
# print("Available columns in 'states' table:", states_table.c.keys())

# print(repr(states_table))

# # Export states_table
# __all__ = ['states_table']
