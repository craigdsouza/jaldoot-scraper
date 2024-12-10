from sqlalchemy.ext.automap import automap_base
from sqlalchemy import create_engine, MetaData, Table
from config.settings import DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DATABASE_URL)
print("Connected to the database.")

# Create a metadata object: metadata
metadata = MetaData() # type: sqlalchemy.sql.schema.MetaData
metadata.reflect(engine)
print("Metadata object created successfully.")

# List of tables to reflect
tables_to_reflect = ['states', 'districts', 'blocks', 'panchayats']

reflected_tables = {}

for table_name in tables_to_reflect:
    try:
        table = Table(table_name, metadata, autoload_with=engine)
        reflected_tables[table_name] = table
        # print(f"Table '{table_name}' reflected successfully.")
        # print(f"Available columns in '{table_name}' table: {table.c.keys()}\n")
    except Exception as e:
        # print(f"Error reflecting table '{table_name}': {e}\n")
        pass

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
