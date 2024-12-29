# main.py

from pathlib import Path
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models import Base, State, District , Block, Panchayat
from modules.scrape import Scraper
from config.settings import (
    EXCEL_FILE, SHEET_NAMES, BASE_URL, logger,
    DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
)

from modules.utils import (
    update_status, 
    begin_scraping_log, 
    end_scraping_log, 
    initialize_driver, 
    sheet_empty, 
    create_excel_file, 
    verify_excel_file,
    count_records,
    get_expected_counts,
    get_db_session
)

from tenacity import RetryError
import pandas as pd
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()
from sqlalchemy import event
from sqlalchemy.engine import Engine

# Log PostgreSQL errors
@event.listens_for(Engine, "handle_error")
def receive_handle_error(exception_context):
    logger.error(f"PostgreSQL error: {exception_context.original_exception}")

def main():
    update_status("Running")
    # Initialize WebDriver
    driver = initialize_driver()
    scraper = Scraper(driver, BASE_URL)

    session = get_db_session()
    engine  = session.get_bind()

    try:
        start_time = begin_scraping_log()

        ##### Scrape the STATE table #####
        # Get count of all states in the State table
        try:
            states = session.query(State)   # <class 'sqlalchemy.orm.query.Query'>
            logger.info(f"States table queried successfully, count of states: {states.count()}")
        except Exception as e:
            logger.error(f"Error querying states: {e}")

        # to-test - delete the states table from the postgres db and check if it gets added back
        if states.count() == 0:
            try:
                logger.info("Scraping state table...")
                state_table = scraper.get_states()
            except RetryError as re:
                logger.error(f"Retry attempts failed for get_states: {re}")
                state_table = pd.DataFrame()  # Assign empty DataFrame
            except Exception as e:
                logger.error(f"Unexpected error during get_states: {e}")
                state_table = pd.DataFrame()
            
            # If state_table has data, save it to postgres table
            if not state_table.empty:
                try:
                    logger.info("Saving state table (pandas df) to postgres table")
                    state_table.to_sql("states", engine, if_exists='replace', index=False)
                    session.commit()
                    logger.info("State table saved to postgres successfully.")
                except Exception as e:
                    logger.error(f"Error saving state table to postgres : {e}")
            else:
                logger.warning("State table is empty. Skipping saving.")
        else:
            logger.info("states postgres table exists and isn't empty. Loading states table from postgres...")
            logger.info(f"States table queried successfully, count of states: {states.count()}")
        
        ##### Scrape the DISTRICT tables #####
        # Get count of all districts in the District table
        try:
            all_districts = session.query(District)   # <class 'sqlalchemy.orm.query.Query'>
            starting_length_districts = all_districts.count()
            logger.info(f"Districts table queried successfully, count of districts: {starting_length_districts}")
        except Exception as e:
            logger.error(f"Error querying districts: {e}")

        # Get list of available states in the District table
        try:
            available_states = session.query(District.__table__.c["States/UT's"]).distinct().all() # <class 'sqlalchemy.orm.query.Query'>
            logger.info(f"available_states type:{type(available_states)} ")  # <class 'list'>
            available_states = [state[0] for state in available_states] # Convert to clean list
            logger.info(f"available distinct states are:{available_states}")
        except Exception as e:
            logger.error("Error getting unique states from all_districts: %s", e)
            available_states = []
        logger.info(f"Loaded {starting_length_districts} districts from {len(available_states)} states in postgres.")
        
        # Find missing states by querying the State table
        try:
            missing_states = session.query(State).filter(~State.__table__.c["States/UT's"].in_(available_states)).all() # <class 'sqlalchemy.orm.query.Query'>
            logger.info(f"no of missing states: {len(missing_states)}")
            # logger.info(f"missing_states are: {missing_states}")
            # logger.info(f"missing_states[0] type: {type(missing_states[0])}") # <class 'models.State'>
            # logger.info(f"missing_states[0] dir: {dir(missing_states[0])}") # ['No. of Panchayat Covered', .., "States/UT's", .., ..]
        except Exception as e:
            logger.error("Error finding missing states: %s", e)
            missing_states = []
        
        if len(missing_states)>0:
            try:
                logger.info("Scraping district tables for missing states...")
                state_list = []

                missing_states_names = [getattr(state, "States/UT's") for state in missing_states]
                missing_states_urls = [getattr(state, "URL") for state in missing_states]        
                state_list = list(zip(missing_states_names, missing_states_urls))
                
                for state, url in state_list:
                    logger.info(f"Scraping districts for state: {state}")
                    try:
                        district_table = scraper.get_districts(state, url)
                    except RetryError as re:
                        logger.error(f"Retry attempts failed for get_districts for state {state}: {re}")
                        district_table = pd.DataFrame()
                    except Exception as e:
                        logger.error(f"Unexpected error during get_districts for state {state}: {e}")
                        district_table = pd.DataFrame()
                    
                    # Save all_districts to postgres if not empty
                    if not district_table.empty:
                        logger.info(f"Scraped {len(district_table)} districts for state {state}.")
                        logger.info("Saving new district tables to postgres...")
                        try:
                            district_table.to_sql("districts", engine, if_exists='append', index=False)
                            session.commit()
                            logger.info("District table saved to postgres successfully.")
                        except Exception as e:
                            logger.error(f"Error saving district table to postgres : {e}")                        
                    else:
                        logger.warning(f"No districts scraped for state {state}. Skipping saving.")
            except Exception as e:
                logger.error(f"Error during district scraping: {e}")
        else:
            logger.warning("No missing states found. Skipping district scraping.")
        
        ##### Scrape the BLOCK tables #####
        # Get count of all blocks in the Block table
        try:
            all_blocks = session.query(Block)
            starting_length_blocks = all_blocks.count()
            logger.info(f"Blocks table queried successfully, count of blocks: {starting_length_blocks}")
        except Exception as e:
            logger.error(f"Error querying blocks: {e}")

        # Get list of available state-district pairs in the Block table
        try:
            available_districts = session.query(Block.__table__.c["States/UT's"], Block.__table__.c["District"]).distinct().all() # <class 'sqlalchemy.orm.query.Query'>
            available_districts = [(district[0], district[1]) for district in available_districts] # Convert to clean list
            logger.info(f"available distinct districts are:{available_districts[0:10]} ...")
        except Exception as e:
            logger.error(f"Error getting unique districts from all_blocks: {e}")
            available_districts = []
        logger.info(f"Loaded {starting_length_blocks} blocks from {len(available_districts)} state-district pairs in postgres.")

        # Find missing state-district pairs by querying the District table
        try:
            subquery = session.query(Block.__table__.c["States/UT's"], Block.__table__.c["District"]).distinct().subquery()
            missing_districts = session.query(District).filter(
                ~session.query(subquery).filter(
                    (subquery.c["States/UT's"] == District.__table__.c["States/UT's"]) &
                    (subquery.c["District"] == District.__table__.c["District"])
                ).exists()
            ).all()
            logger.info(f"no of missing state-district pairs: {len(missing_districts)}")
        except Exception as e:
            logger.error(f"Error finding missing state-district pairs: {e}")
            missing_districts = []

        if len(missing_districts)>0:
            try:
                logger.info("Scraping block tables for missing state-district combinations...")
                district_list = []

                missing_states_names = [getattr(district, "States/UT's") for district in missing_districts]
                missing_districts_names = [getattr(district, "District") for district in missing_districts]
                missing_districts_urls = [getattr(district, "URL") for district in missing_districts]
                district_list = list(zip(missing_states_names, missing_districts_names, missing_districts_urls))

                for state, district, url in district_list:
                    logger.info(f"Scraping blocks for {district} , {state}")
                    try:
                        block_table = scraper.get_blocks(state, district, url)
                    except RetryError as re:
                        logger.error(f"Retry attempts failed for get_blocks for {district} , {state}: {re}")
                        block_table = pd.DataFrame()
                    except Exception as e:
                        logger.error(f"Unexpected error during get_blocks for {district} , {state}: {e}")
                        block_table = pd.DataFrame()
                    
                    # Save all_blocks to postgres if not empty
                    if not block_table.empty:
                        logger.info(f"Scraped {len(block_table)} blocks for {district} , {state}.")
                        logger.info("Saving new block tables to postgres...")
                        try:
                            block_table.to_sql("blocks", engine, if_exists='append', index=False)
                            session.commit()
                            logger.info("Block table saved to postgres successfully.")
                        except Exception as e:
                            logger.error(f"Error saving block table to postgres : {e}")                        
                    else:
                        logger.warning(f"No blocks scraped for {district} , {state}. Skipping saving.")
            except Exception as e:
                logger.error(f"Error during block scraping: {e}")
        else:
            logger.warning("No missing state-district pairs found. Skipping block scraping.")

        ##### Scrape the PANCHAYAT tables #####
        # Get count of all panchayats in the Panchayat table
        try:
            all_panchayats = session.query(Panchayat)
            starting_length_panchayats = all_panchayats.count()
            logger.info(f"Panchayats table queried successfully, count of panchayats: {starting_length_panchayats}")
        except Exception as e:
            logger.error(f"Error querying panchayats: {e}")

        # Get list of available state-district-block pairs in the Panchayat table
        try:
            available_blocks = session.query(Panchayat.__table__.c["States/UT's"], Panchayat.__table__.c["District"], Panchayat.__table__.c["Block"]).distinct().all() # <class 'sqlalchemy.orm.query.Query'>
            available_blocks = [(block[0], block[1], block[2]) for block in available_blocks] # Convert to clean list
            logger.info(f"available distinct blocks are:{available_blocks[0:10]} ...")
        except Exception as e:
            logger.error(f"Error getting unique blocks from all_panchayats: {e}")
            available_blocks = []
        logger.info(f"Loaded {starting_length_panchayats} panchayats from {len(available_blocks)} state-district-block pairs in postgres.")

        # Find missing state-district-block pairs by querying the Block table
        try:
            subquery = session.query(Panchayat.__table__.c["States/UT's"], Panchayat.__table__.c["District"], Panchayat.__table__.c["Block"]).distinct().subquery()
            missing_panchayats = session.query(Block).filter(
                ~session.query(subquery).filter(
                    (subquery.c["States/UT's"] == Block.__table__.c["States/UT's"]) &
                    (subquery.c["District"] == Block.__table__.c["District"]) &
                    (subquery.c["Block"] == Block.__table__.c["Block"])
                ).exists()
            ).all()
            logger.info(f"no of missing state-district-block pairs: {len(missing_panchayats)}")
        except Exception as e:
            logger.error(f"Error finding missing state-district-block pairs: {e}")
            missing_panchayats = []

        if len(missing_panchayats)>0:
            try:
                logger.info("Scraping panchayat tables for missing state-district-block combinations...")
                block_list = []

                missing_states_names = [getattr(panchayat, "States/UT's") for panchayat in missing_panchayats]
                missing_districts_names = [getattr(panchayat, "District") for panchayat in missing_panchayats]
                missing_blocks_names = [getattr(panchayat, "Block") for panchayat in missing_panchayats]
                missing_blocks_urls = [getattr(panchayat, "URL") for panchayat in missing_panchayats]
                block_list = list(zip(missing_states_names, missing_districts_names, missing_blocks_names, missing_blocks_urls))

                for state, district, block, url in block_list:
                    logger.info(f"Scraping panchayats for {block} , {district} , {state}")
                    try:
                        panchayat_table = scraper.get_panchayats(state, district, block, url)
                    except RetryError as re:
                        logger.error(f"Retry attempts failed for get_panchayats for {block} , {district} , {state}: {re}")
                        panchayat_table = pd.DataFrame()
                    except Exception as e:
                        logger.error(f"Unexpected error during get_panchayats for {block} , {district} , {state}: {e}")
                        panchayat_table = pd.DataFrame()
                    
                    # Save all_panchayats to postgres if not empty
                    if not panchayat_table.empty:
                        logger.info(f"Scraped {len(panchayat_table)} panchayats for {block} , {district} , {state}.")
                        
                        # Convert empty strings in numeric columns to NaN, then fill NaN with None
                        logger.info("Converting empty strings to NaN>None in numeric columns...")
                        numeric_columns = ["Well Diameter(In Feet)", "Pre Monsoon Water Level(In Feet)", "Pre Monsoon Latitude", "Pre Monsoon Longitude"]
                        for column in numeric_columns:
                            panchayat_table[column] = pd.to_numeric(panchayat_table[column], errors='coerce')
                            panchayat_table[column].fillna(None, inplace=True)    # SINCE POSTGRES DOESN'T ALLOW NaN VALUES IN INTEGER COLUMNS

                        logger.info("Saving new panchayat tables to postgres...")
                        try:
                            panchayat_table.to_sql("panchayats", engine, if_exists='append', index=False)
                            session.commit()
                            logger.info("Panchayat table saved to postgres successfully.")
                        except Exception as e:
                            logger.error(f"Error saving panchayat table to postgres : {e}")                        
                    else:
                        logger.warning(f"No panchayats scraped for {block} , {district} , {state}. Skipping saving.")
            except Exception as e:
                logger.error(f"Error during panchayat scraping: {e}")
        else:
            logger.warning("No missing state-district-block pairs found. Skipping panchayat scraping.")
    
    except Exception as e:
        logger.error("Error during MAIN scraping process: %s", e)
        update_status("Error", str(e))
    finally:
        driver.quit()
        session.close()
        logger.info("WebDriver closed.")
        end_scraping_log(start_time)
        update_status("Stopped","Scraper completed successfully")

if __name__ == "__main__":
    main()