import argparse
import time
import random
from sqlalchemy import MetaData, Table, select, update, or_, and_
from modules.utils import initialize_driver, get_db_session
from modules.scrape import Scraper
from normalizers.blocks import normalize_blocks
from loaders.upsert import upsert_blocks
from config.settings import logger


def fetch_districts_for_season(engine, season, include_all=False, state_filter: str | None = None, district_filter: str | None = None):
    """Return list of (state_ut, district, url) for a season from the districts table.
    If include_all is False, only returns rows where scraped is NULL or 'n'.
    Optionally filter by state_ut and/or district (case-insensitive exact match).
    """
    metadata = MetaData()
    districts = Table("districts", metadata, autoload_with=engine)
    cond = [districts.c.season == season]
    if state_filter:
        cond.append(districts.c.state_ut.ilike(state_filter))
    if district_filter:
        cond.append(districts.c.district.ilike(district_filter))
    stmt = select(districts.c.state_ut, districts.c.district, districts.c.url).where(and_(*cond))
    if not include_all and hasattr(districts.c, "scraped"):
        stmt = stmt.where(or_(districts.c.scraped.is_(None), districts.c.scraped == 'n'))
    with engine.connect() as conn:
        rows = conn.execute(stmt).fetchall()
    return [(r.state_ut, r.district, r.url) for r in rows]


def run_for_season(season: str, include_all: bool, state_filter: str | None, district_filter: str | None) -> int:
    session = get_db_session()
    engine = session.get_bind()

    district_rows = fetch_districts_for_season(engine, season, include_all=include_all, state_filter=state_filter, district_filter=district_filter)
    if not district_rows:
        logger.warning(
            "No districts found in DB for season %s. Run the districts scraper first.",
            season,
        )
        return 0

    driver = initialize_driver()
    scraper = Scraper(driver, base_url="")

    total_affected = 0
    processed = 0
    try:
        for state_ut, district, url in district_rows:
            # Polite per-request delay with jitter
            time.sleep(random.uniform(2, 5))
            if processed and processed % 25 == 0:
                cooldown = random.uniform(120, 180)
                logger.info("Cooling down for %.1fs after %d districts", cooldown, processed)
                time.sleep(cooldown)
            logger.info("Scraping blocks for %s / %s (%s)", state_ut, district, season)
            try:
                df_raw = scraper.get_blocks(state_ut, district, url)
            except Exception as e:
                logger.error("Error scraping blocks for %s / %s (%s): %s", state_ut, district, season, e)
                metadata = MetaData()
                districts_tbl = Table("districts", metadata, autoload_with=engine)
                upd = (
                    update(districts_tbl)
                    .where(
                        (districts_tbl.c.state_ut == state_ut)
                        & (districts_tbl.c.district == district)
                        & (districts_tbl.c.season == season)
                    )
                    .values(scraped="n")
                )
                with engine.begin() as conn:
                    conn.execute(upd)
                processed += 1
                continue

            if df_raw.empty:
                logger.warning("0 blocks found for %s / %s (%s)", state_ut, district, season)
                metadata = MetaData()
                districts_tbl = Table("districts", metadata, autoload_with=engine)
                upd = (
                    update(districts_tbl)
                    .where(
                        (districts_tbl.c.state_ut == state_ut)
                        & (districts_tbl.c.district == district)
                        & (districts_tbl.c.season == season)
                    )
                    .values(scraped="e")
                )
                with engine.begin() as conn:
                    conn.execute(upd)
                processed += 1
                continue

            df_norm = normalize_blocks(df_raw, None, season, state_ut, district)
            if df_norm.empty:
                logger.warning("0 usable block rows after normalization for %s / %s (%s)", state_ut, district, season)
                metadata = MetaData()
                districts_tbl = Table("districts", metadata, autoload_with=engine)
                upd = (
                    update(districts_tbl)
                    .where(
                        (districts_tbl.c.state_ut == state_ut)
                        & (districts_tbl.c.district == district)
                        & (districts_tbl.c.season == season)
                    )
                    .values(scraped="e")
                )
                with engine.begin() as conn:
                    conn.execute(upd)
                processed += 1
                continue

            affected = upsert_blocks(engine, df_norm)
            session.commit()
            logger.info(
                "Upserted %s rows for %s / %s (%s)", affected, state_ut, district, season
            )
            metadata = MetaData()
            districts_tbl = Table("districts", metadata, autoload_with=engine)
            upd = (
                update(districts_tbl)
                .where(
                    (districts_tbl.c.state_ut == state_ut)
                    & (districts_tbl.c.district == district)
                    & (districts_tbl.c.season == season)
                )
                .values(scraped="y")
            )
            with engine.begin() as conn:
                conn.execute(upd)
            total_affected += affected
            processed += 1

        return total_affected
    finally:
        driver.quit()


def main():
    parser = argparse.ArgumentParser(
        description="Scrape blocks for districts already stored in the DB"
    )
    parser.add_argument("seasons", nargs="*", help="Season(s) to process. If omitted with --state/--district, all seasons matching those filters are processed.")
    parser.add_argument("--state", help="Process only the specified state_ut (case-insensitive exact match)")
    parser.add_argument("--district", help="Process only the specified district (case-insensitive exact match)")
    parser.add_argument(
        "--all",
        action="store_true",
        help="Re-scrape all districts for the season, including those already marked scraped='y'",
    )
    args = parser.parse_args()

    session = get_db_session()
    engine = session.get_bind()
    try:
        if args.district and not args.state:
            parser.error("--district requires --state to disambiguate identical district names across states")

        seasons = args.seasons
        if not seasons and (args.state or args.district):
            # Gather all seasons present for the given filters
            metadata = MetaData()
            districts = Table("districts", metadata, autoload_with=engine)
            cond = []
            if args.state:
                cond.append(districts.c.state_ut.ilike(args.state))
            if args.district:
                cond.append(districts.c.district.ilike(args.district))
            stmt = select(districts.c.season).where(and_(*cond)) if cond else select(districts.c.season)
            stmt = stmt.distinct()
            with engine.connect() as conn:
                seasons = [r.season for r in conn.execute(stmt).fetchall()]
            if not seasons:
                logger.warning("No seasons found in DB for the given filters: state=%s, district=%s", args.state, args.district)
                return

        if not seasons:
            logger.error("No seasons provided. Provide seasons or use --state/--district to infer seasons for those filters.")
            return

        grand_total = 0
        for season in seasons:
            try:
                grand_total += run_for_season(season, include_all=args.all, state_filter=args.state, district_filter=args.district)
            except Exception as e:
                logger.error("Season %s failed: %s", season, e)
        logger.info("Blocks total upserted across seasons: %s", grand_total)
    finally:
        session.close()


if __name__ == "__main__":
    main()




