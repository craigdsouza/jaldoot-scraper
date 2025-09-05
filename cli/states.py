import argparse
import sys
import pandas as pd
from modules.utils import initialize_driver, get_db_session
from modules.scrape import Scraper
from config.season_registry import SEASONS
from normalizers.states import normalize_states
from loaders.upsert import upsert_states
from config.settings import logger


def run_for_season(season: str) -> int:
    if season not in SEASONS:
        logger.error("Unknown season: %s", season)
        return 0
    sd = SEASONS[season]
    driver = initialize_driver()
    try:
        scraper = Scraper(driver, sd.url)
        df = scraper.get_states()
        if df.empty:
            logger.warning("No data scraped for season %s", season)
            return 0
        df_norm = normalize_states(df, sd, season)
        session = get_db_session()
        engine = session.get_bind()
        affected = upsert_states(engine, df_norm)
        session.commit()
        logger.info("Season %s upserted rows: %s", season, affected)
        return affected
    finally:
        driver.quit()


def main(argv=None):
    parser = argparse.ArgumentParser(description="Scrape and upsert states for one or more seasons")
    parser.add_argument("seasons", nargs="*", help="Seasons to process. If empty, all known seasons are used.")
    args = parser.parse_args(argv)

    seasons = args.seasons or list(SEASONS.keys())
    total = 0
    for s in seasons:
        try:
            total += run_for_season(s)
        except Exception as e:
            logger.error("Season %s failed: %s", s, e)
    logger.info("Total upserted across seasons: %s", total)


if __name__ == "__main__":
    main()




