import pandas as pd
from sqlalchemy import create_engine
from config.settings import DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME

# Database connection
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DATABASE_URL)

# Load Excel file
excel_file = '../data/jaldoot-remote-20241203-000700.xlsx'

# Import states data
states_df = pd.read_excel(excel_file, sheet_name='state')
states_df.to_sql('states', engine, if_exists='append', index=False)

# Import districts data
districts_df = pd.read_excel(excel_file, sheet_name='district')
districts_df.to_sql('districts', engine, if_exists='append', index=False)

# Import blocks data
blocks_df = pd.read_excel(excel_file, sheet_name='block')
blocks_df.to_sql('blocks', engine, if_exists='append', index=False)

# Import panchayats data
panchayats_df = pd.read_excel(excel_file, sheet_name='panchayat')
panchayats_df.to_sql('panchayats', engine, if_exists='append', index=False)

print("Data imported successfully!")