import argparse
from sqlalchemy import MetaData, Table, select, update, or_, and_
from modules.utils import initialize_driver, get_db_session
from modules.scrape import Scraper
from normalizers.districts import normalize_districts
from loaders.upsert import upsert_districts
from config.settings import logger


def fetch_states_for_season(engine, season, include_all=False, state_filter: str | None = None):
    """Return list of (state_ut, url) for a season from the states table.
    If include_all is False, only returns rows where scraped is NULL or 'n'.
    Optionally filter by a specific state_ut (case-insensitive exact match).
    """
    metadata = MetaData()
    states = Table("states", metadata, autoload_with=engine)
    cond = [states.c.season == season]
    if state_filter:
        cond.append(states.c.state_ut.ilike(state_filter))
    stmt = select(states.c.state_ut, states.c.url).where(and_(*cond))
    if not include_all and hasattr(states.c, "scraped"):
        stmt = stmt.where(or_(states.c.scraped.is_(None), states.c.scraped == 'n'))
    with engine.connect() as conn:
        rows = conn.execute(stmt).fetchall()
    return [(r.state_ut, r.url) for r in rows]


def run_for_season(season: str, include_all: bool, state_filter: str | None) -> int:
    session = get_db_session()
    engine = session.get_bind()
    state_rows = fetch_states_for_season(engine, season, include_all=include_all, state_filter=state_filter)
    if not state_rows:
        logger.warning("No states that match the filter (scraped and state_filter) found in DB for season %s.", season)
        return 0

    driver = initialize_driver()
    scraper = Scraper(driver, base_url="")  # base_url unused for district calls

    total_affected = 0
    try:
        for state_ut, url in state_rows:
            logger.info("Scraping districts for %s (%s)", state_ut, season)
            try:
                df_raw = scraper.get_districts(state_ut, url)
            except Exception as e:
                logger.error("Error scraping districts for %s (%s): %s", state_ut, season, e)
                metadata = MetaData()
                states = Table("states", metadata, autoload_with=engine)
                upd = update(states).where((states.c.state_ut == state_ut) & (states.c.season == season)).values(scraped='n')
                with engine.begin() as conn:
                    conn.execute(upd)
                continue

            if df_raw.empty:
                logger.warning("0 districts found for %s (%s)", state_ut, season)
                # mark empty result
                metadata = MetaData()
                states = Table("states", metadata, autoload_with=engine)
                upd = update(states).where((states.c.state_ut == state_ut) & (states.c.season == season)).values(scraped='e')
                with engine.begin() as conn:
                    conn.execute(upd)
                continue
            df_norm = normalize_districts(df_raw, None, season, state_ut)
            if df_norm.empty:
                logger.warning("0 usable district rows after normalization for %s (%s)", state_ut, season)
                metadata = MetaData()
                states = Table("states", metadata, autoload_with=engine)
                upd = update(states).where((states.c.state_ut == state_ut) & (states.c.season == season)).values(scraped='e')
                with engine.begin() as conn:
                    conn.execute(upd)
                continue
            affected = upsert_districts(engine, df_norm)
            session.commit()
            logger.info("Upserted %s rows for %s (%s)", affected, state_ut, season)
            # mark success on state
            metadata = MetaData()
            states = Table("states", metadata, autoload_with=engine)
            upd = update(states).where((states.c.state_ut == state_ut) & (states.c.season == season)).values(scraped='y')
            with engine.begin() as conn:
                conn.execute(upd)
            total_affected += affected
        return total_affected
    finally:
        driver.quit()


def main():
    parser = argparse.ArgumentParser(description="Scrape districts for states already stored in the DB")
    parser.add_argument("seasons", nargs="*", help="Season(s) to process. If omitted with --state, all seasons for that state are processed.")
    parser.add_argument("--state", help="Process only the specified state_ut (case-insensitive exact match)")
    parser.add_argument(
        "--all",
        action="store_true",
        help="Re-scrape all states for the season, including those already marked scraped='y'",
    )
    args = parser.parse_args()

    session = get_db_session()
    engine = session.get_bind()
    try:
        seasons = args.seasons
        if not seasons and args.state:
            # Gather all seasons present for the given state
            metadata = MetaData()
            states = Table("states", metadata, autoload_with=engine)
            q = select(states.c.season).where(states.c.state_ut.ilike(args.state)).distinct()
            with engine.connect() as conn:
                seasons = [r.season for r in conn.execute(q).fetchall()]
            if not seasons:
                logger.warning("No seasons found in DB for state %s", args.state)
                return

        if not seasons:
            logger.error("No seasons provided. Provide seasons or use --state to infer seasons for that state.")
            return

        grand_total = 0
        for season in seasons:
            try:
                grand_total += run_for_season(season, include_all=args.all, state_filter=args.state)
            except Exception as e:
                logger.error("Season %s failed: %s", season, e)
        logger.info("Districts total upserted across seasons: %s", grand_total)
    finally:
        session.close()


if __name__ == "__main__":
    main()


