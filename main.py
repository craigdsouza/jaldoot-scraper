# main.py

from pathlib import Path
from modules.scrape import Scraper
from config.settings import EXCEL_FILE, SHEET_NAMES, BASE_URL, logger
from modules.utils import (
    update_status, 
    begin_scraping_log, 
    end_scraping_log, 
    initialize_driver, 
    sheet_empty, 
    create_excel_file, 
    verify_excel_file,
    count_records,
    get_expected_counts
)

from tenacity import RetryError
import pandas as pd
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

def main():
    update_status("Running")
    # Initialize WebDriver
    driver = initialize_driver()
    scraper = Scraper(driver, BASE_URL)
    try:
        # Create Excel file and sheets if it doesn't exist
        if not EXCEL_FILE.exists():
            create_excel_file(EXCEL_FILE, SHEET_NAMES)
        else:
            # Ensure all required sheets exist; create missing ones
            verify_excel_file(EXCEL_FILE, SHEET_NAMES)

        start_time = begin_scraping_log()

        ##### Scrape the STATE table #####
        if sheet_empty(EXCEL_FILE, SHEET_NAMES[0]):
            try:
                logger.info("Scraping state table...")
                state_table = scraper.get_states()
            except RetryError as re:
                logger.error("Retry attempts failed for get_states: %s", re)
                state_table = pd.DataFrame()  # Assign empty DataFrame
            except Exception as e:
                logger.error("Unexpected error during get_states: %s", e)
                state_table = pd.DataFrame()
            
            # Save state_table to Excel if not empty
            if not state_table.empty:
                try:
                    logger.info("Saving state table to Excel...")
                    with pd.ExcelWriter(EXCEL_FILE, mode='a', if_sheet_exists='replace', engine='openpyxl') as writer:
                        state_table.to_excel(writer, sheet_name=SHEET_NAMES[0], index=False)
                    logger.info("State table saved successfully.")
                except Exception as e:
                    logger.error("Error saving state table to Excel: %s", e)
            else:
                logger.warning("State table is empty. Skipping saving.")
        else:
            logger.info("State sheet exists and isn't empty. Loading state table from Excel...")
            try:
                state_table = pd.read_excel(EXCEL_FILE, sheet_name=SHEET_NAMES[0])
                logger.info("Loaded %d states from Excel.", len(state_table))
            except Exception as e:
                logger.error("Error loading state table from Excel: %s", e)
                state_table = pd.DataFrame()
        
        ##### Scrape the DISTRICT tables #####
        all_districts = pd.ExcelFile(EXCEL_FILE).parse(SHEET_NAMES[1])
        starting_length_districts = len(all_districts)

        # If missing states-district pairs are found, scrape
        try:
            available_states = all_districts["States/UT\'s"].unique()
        except Exception as e:
            logger.error("Error getting unique states from all_districts: %s", e)
            available_states = []
        logger.info("Loaded %d districts from %d states in Excel.", starting_length_districts, len(available_states))
        missing_states = state_table[~state_table["States/UT\'s"].isin(available_states)]
        logger.info("Found %d missing states.", len(missing_states))

        if not missing_states.empty:
            try:
                logger.info("Scraping district tables for missing states...")
                state_list = list(zip(missing_states["States/UT\'s"], missing_states['URL']))
                
                for state, url in state_list:
                    logger.info("Scraping districts for state: %s", state)
                    try:
                        district_table = scraper.get_districts(state, url)
                    except RetryError as re:
                        logger.error("Retry attempts failed for get_districts for state %s: %s", state, re)
                        district_table = pd.DataFrame()
                    except Exception as e:
                        logger.error("Unexpected error during get_districts for state %s: %s", state, e)
                        district_table = pd.DataFrame()
                    
                    if not district_table.empty:
                        all_districts = pd.concat([all_districts, district_table], ignore_index=True)
                        logger.info("Scraped %d districts for state %s.", len(district_table), state)
                    else:
                        logger.warning("No districts scraped for state %s.", state)
                
                # Save all_districts to Excel if not empty
                if len(all_districts) > starting_length_districts:
                    try:
                        logger.info("Saving new district tables to Excel...")
                        with pd.ExcelWriter(EXCEL_FILE, mode='a', if_sheet_exists='replace', engine='openpyxl') as writer:
                            all_districts.to_excel(writer, sheet_name=SHEET_NAMES[1], index=False)
                        logger.info("District table saved successfully.")
                    except Exception as e:
                        logger.error("Error saving district table to Excel: %s", e)
                else:
                    logger.warning("No new district data scraped. Skipping saving.")
            except Exception as e:
                logger.error("Error during district scraping: %s", e)
        else:
            logger.warning("No missing states found. Skipping district scraping.")
        
        ##### Scrape the BLOCK tables #####

        # Load existing blocks from Excel
        all_blocks = pd.ExcelFile(EXCEL_FILE).parse(SHEET_NAMES[2])
        starting_length_blocks = len(all_blocks)

        try:
            # Select relevant columns for comparison
            all_blocks_pairs = all_blocks[['States/UT\'s', 'District']]
            unique_state_district_pairs = all_blocks_pairs.drop_duplicates()
        except Exception as e:
            logger.error("Error accessing 'States/UT\'s and 'District' columns in all_blocks: %s", e)
            unique_state_district_pairs = pd.DataFrame(columns=["States/UT\'s", "District"])

        logger.info("Loaded %d blocks from %d state-district pairs in Excel.", starting_length_blocks, len(unique_state_district_pairs))

        # Perform a left merge to identify missing state-district pairs
        missing_districts = all_districts.merge(
            unique_state_district_pairs,
            on=["States/UT\'s", "District"],
            how='left',
            indicator=True
        )

        # Filter rows that did not find a match in all_blocks
        missing_districts = missing_districts[missing_districts['_merge'] == 'left_only']

        # Drop the merge indicator column
        missing_districts = missing_districts.drop(columns=['_merge'])

        logger.info("Found %d missing state-district combinations.", len(missing_districts))

        if not missing_districts.empty:
            try:
                logger.info("Scraping block tables for missing  state-district combinations...")
                # Create a list of tuples: (State/UT\'s, District, URL)
                district_list = list(zip(
                    missing_districts["States/UT\'s"],
                    missing_districts["District"],
                    missing_districts['URL']
                ))

                for state, district, url in district_list:
                    logger.info("Scraping data for %s , %s", district, state)
                    try:
                        block_table = scraper.get_blocks(state, district, url)
                    except RetryError as re:
                        logger.error("Retry attempts failed for get_blocks for %s , %s : %s", district, state, re)
                        block_table = pd.DataFrame()
                    except Exception as e:
                        logger.error("Unexpected error during get_blocks for %s , %s : %s", district, state, e)
                        block_table = pd.DataFrame()

                    if not block_table.empty:
                        all_blocks = pd.concat([all_blocks, block_table], ignore_index=True)
                        logger.info("Scraped %d records for %s , %s", len(block_table), district, state)
                    else:
                        logger.warning("No data scraped for %s , %s ", district, state)

                # Save all_blocks to Excel if new data was added
                if len(all_blocks) > starting_length_blocks:
                    try:
                        logger.info("Saving new block data to Excel...")
                        with pd.ExcelWriter(EXCEL_FILE, mode='a', if_sheet_exists='replace', engine='openpyxl') as writer:
                            all_blocks.to_excel(writer, sheet_name=SHEET_NAMES[2], index=False)
                        logger.info("Block data saved successfully.")
                    except Exception as e:
                        logger.error("Error saving block data to Excel: %s", e)
                else:
                    logger.warning("No new block data scraped. Skipping saving.")
            except Exception as e:
                logger.error("Error during block scraping: %s", e)
        else:
            logger.warning("No missing districts found. Skipping block scraping.")

        ##### Scrape the PANCHAYAT tables #####

        # Load existing panchayats from Excel
        all_panchayats = pd.ExcelFile(EXCEL_FILE).parse(SHEET_NAMES[3])
        starting_length_panchayats = len(all_panchayats)

        # If missing states-district-block pairs are found, scrape
        try:
            # Select relevant columns for comparison
            all_panchayat_pairs = all_panchayats[['States/UT\'s', 'District', 'Block']]
            unique_state_district_block_pairs = all_panchayat_pairs.drop_duplicates()
        except Exception as e:
            logger.error("Error accessing 'States/UT\'s', 'District' and 'Block' columns in all_blocks: %s", e)
            unique_state_district_block_pairs = pd.DataFrame(columns=["States/UT\'s", "District", "Block"])

        logger.info("Loaded %d panchayats from %d state-district-block pairs in Excel.", 
                    starting_length_panchayats, 
                    len(unique_state_district_block_pairs))


        # Perform a left merge to identify missing state-district pairs
        missing_blocks = all_blocks.merge(
            unique_state_district_block_pairs,
            on=["States/UT\'s", "District","Block"],
            how='left',
            indicator=True
        )

        # Filter rows that did not find a match in all_blocks
        missing_blocks = missing_blocks[missing_blocks['_merge'] == 'left_only']

        # Drop the merge indicator column
        missing_blocks = missing_blocks.drop(columns=['_merge'])
        
        # Create state-district pairs using missing_blocks to loop through.
        # Select relevant columns for comparison

        if not missing_blocks.empty:
            missing_blocks_pairs = missing_blocks[['States/UT\'s', 'District']]
            unique_missing_block_pairs = missing_blocks_pairs.drop_duplicates()
            logger.info("Found %d missing state-district-block combinations representing %d unique state-district pairs.",
                         len(missing_blocks_pairs),
                         len(unique_missing_block_pairs))

            # loop through first two rows of unique_missing_block_pairs
            for index, row in unique_missing_block_pairs.iterrows():
                state = row['States/UT\'s']
                district = row['District']
                logger.info("--------------------------------------------------")
                logger.info("Scraping panchayat data for %s , %s", district, state)
                blocks = missing_blocks.loc[(missing_blocks['States/UT\'s'] == state) & (missing_blocks['District'] == district), 'Block']
                url = missing_blocks.loc[(missing_blocks['States/UT\'s'] == state) & (missing_blocks['District'] == district), 'URL']
                logger.info("No of blocks: %d", len(blocks))
                logger.info("--------------------------------------------------")

                for block, url in zip(blocks, url):
                    update_status("Running")
                    try:
                        panchayat_table = scraper.get_panchayats(state, district, block, url)
                    except RetryError as re:
                        logger.error("Retry attempts failed for get_panchayats for %s , %s , %s : %s", block, district, state, re)
                        panchayat_table = pd.DataFrame()

                    except Exception as e:
                        logger.error("Unexpected error during get_panchayats for %s , %s , %s : %s", block, district, state, e)
                        panchayat_table = pd.DataFrame()

                    if not panchayat_table.empty:
                        all_panchayats = pd.concat([all_panchayats, panchayat_table], ignore_index=True)
                        logger.info("Scraped %d records for %s , %s , %s and added to all_panchayats df", len(panchayat_table), block, district, state)
                    else:
                        logger.warning("No data scraped for %s , %s , %s ", block, district, state)

                # Save all_panchayats to Excel if new data was added
                if len(all_panchayats) > starting_length_panchayats:
                    try:
                        logger.info(f"Saving new panchayat data of {state} , {district} to Excel...")
                        with pd.ExcelWriter(EXCEL_FILE, mode='a', if_sheet_exists='replace', engine='openpyxl') as writer:
                            all_panchayats.to_excel(writer, sheet_name=SHEET_NAMES[3], index=False)
                        logger.info(f"Panchayat data of {state} , {district} saved successfully.")
                        
                    except Exception as e:
                        logger.error(f"Error saving panchayat data of {state} , {district} to Excel: {e}")
                else:
                    logger.warning("No new panchayat data scraped for {state} , {district}. Skipping saving.")
        else:
            logger.warning("No missing blocks found. Skipping panchayat scraping.")
    
    except Exception as e:
        logger.error("Error during MAIN scraping process: %s", e)
        update_status("Error", str(e))
    finally:
        driver.quit()
        logger.info("WebDriver closed.")
        end_scraping_log(start_time)
        update_status("Stopped","Scraper completed successfully")

if __name__ == "__main__":
    main()