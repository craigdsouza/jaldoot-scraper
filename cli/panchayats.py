import argparse
import time
import random
from sqlalchemy import MetaData, Table, select, update, and_, or_
from modules.utils import initialize_driver, get_db_session
from modules.scrape import Scraper
from normalizers.panchayats import normalize_panchayats, normalize_well_measurements
from loaders.upsert import upsert_panchayats, upsert_well_measurements
from config.settings import logger


def fetch_blocks_for_season(
    engine,
    season,
    include_all=False,
    state_filter: str | None = None,
    district_filter: str | None = None,
    block_filter: str | None = None,
):
    """Return list of (state_ut, district, block, url) for a season from the blocks table.
    If include_all is False, only returns rows where scraped is NULL or 'n'.
    Supports optional case-insensitive exact filters for state/district/block.
    """
    metadata = MetaData()
    blocks = Table("blocks", metadata, autoload_with=engine)
    cond = [blocks.c.season == season]
    if state_filter:
        cond.append(blocks.c.state_ut.ilike(state_filter))
    if district_filter:
        cond.append(blocks.c.district.ilike(district_filter))
    if block_filter:
        cond.append(blocks.c.block.ilike(block_filter))

    stmt = select(blocks.c.state_ut, blocks.c.district, blocks.c.block, blocks.c.url).where(and_(*cond))
    if not include_all and hasattr(blocks.c, "scraped"):
        stmt = stmt.where(or_(blocks.c.scraped.is_(None), blocks.c.scraped == "n"))
    with engine.connect() as conn:
        rows = conn.execute(stmt).fetchall()
    return [(r.state_ut, r.district, r.block, r.url) for r in rows]


def run_for_season(
    season: str,
    include_all: bool,
    state_filter: str | None,
    district_filter: str | None,
    block_filter: str | None,
):
    session = get_db_session()
    engine = session.get_bind()

    block_rows = fetch_blocks_for_season(
        engine,
        season,
        include_all=include_all,
        state_filter=state_filter,
        district_filter=district_filter,
        block_filter=block_filter,
    )
    if not block_rows:
        logger.warning(
            "No blocks found in DB for season %s (state=%s, district=%s, block=%s). Run the blocks scraper first or check your filters.",
            season,
            state_filter or "*",
            district_filter or "*",
            block_filter or "*",
        )
        return 0

    driver = initialize_driver()
    scraper = Scraper(driver, base_url="")

    total_affected = 0
    processed = 0
    try:
        for state_ut, district, block, url in block_rows:
            time.sleep(random.uniform(2, 5))
            if processed and processed % 50 == 0:
                cooldown = random.uniform(120, 180)
                logger.info("Cooling down for %.1fs after %d blocks", cooldown, processed)
                time.sleep(cooldown)

            logger.info("\n\n")
            logger.info("Scraping panchayats for %s / %s / %s (%s)", state_ut, district, block, season)
            try:
                df_raw = scraper.get_panchayats(state_ut, district, block, url)
            except Exception as e:
                logger.error(
                    "Error scraping panchayats for %s / %s / %s (%s): %s",
                    state_ut,
                    district,
                    block,
                    season,
                    e,
                )
                metadata = MetaData()
                blocks_tbl = Table("blocks", metadata, autoload_with=engine)
                upd = (
                    update(blocks_tbl)
                    .where(
                        (blocks_tbl.c.state_ut == state_ut)
                        & (blocks_tbl.c.district == district)
                        & (blocks_tbl.c.block == block)
                        & (blocks_tbl.c.season == season)
                    )
                    .values(scraped="n", scraped_count=0)
                )
                with engine.begin() as conn:
                    conn.execute(upd)
                processed += 1
                continue

            if df_raw.empty:
                logger.warning(
                    "0 panchayat rows found for %s / %s / %s (%s)",
                    state_ut,
                    district,
                    block,
                    season,
                )
                metadata = MetaData()
                blocks_tbl = Table("blocks", metadata, autoload_with=engine)
                upd = (
                    update(blocks_tbl)
                    .where(
                        (blocks_tbl.c.state_ut == state_ut)
                        & (blocks_tbl.c.district == district)
                        & (blocks_tbl.c.block == block)
                        & (blocks_tbl.c.season == season)
                    )
                    .values(scraped="e", scraped_count=0)
                )
                with engine.begin() as conn:
                    conn.execute(upd)
                processed += 1
                continue

            df_panch = normalize_panchayats(df_raw, None, season, state_ut, district, block)
            df_wells = normalize_well_measurements(df_raw, season, state_ut, district, block)
            if df_panch.empty and df_wells.empty:
                logger.warning(
                    "0 usable rows after normalization for %s / %s / %s (%s)",
                    state_ut,
                    district,
                    block,
                    season,
                )
                metadata = MetaData()
                blocks_tbl = Table("blocks", metadata, autoload_with=engine)
                upd = (
                    update(blocks_tbl)
                    .where(
                        (blocks_tbl.c.state_ut == state_ut)
                        & (blocks_tbl.c.district == district)
                        & (blocks_tbl.c.block == block)
                        & (blocks_tbl.c.season == season)
                    )
                    .values(scraped="e", scraped_count=0)
                )
                with engine.begin() as conn:
                    conn.execute(upd)
                processed += 1
                continue

            affected_panch = upsert_panchayats(engine, df_panch) if not df_panch.empty else 0
            affected_wells = upsert_well_measurements(engine, df_wells) if not df_wells.empty else 0
            session.commit()
            logger.info(
                "Upserted panchayats=%s, well_measurements=%s for %s / %s / %s (%s)",
                affected_panch,
                affected_wells,
                state_ut,
                district,
                block,
                season,
            )
            metadata = MetaData()
            blocks_tbl = Table("blocks", metadata, autoload_with=engine)
            upd = (
                update(blocks_tbl)
                .where(
                    (blocks_tbl.c.state_ut == state_ut)
                    & (blocks_tbl.c.district == district)
                    & (blocks_tbl.c.block == block)
                    & (blocks_tbl.c.season == season)
                )
                .values(scraped="y", scraped_count=affected_wells)
            )
            with engine.begin() as conn:
                conn.execute(upd)
            total_affected += (affected_panch + affected_wells)
            processed += 1

        return total_affected
    finally:
        driver.quit()


def main():
    parser = argparse.ArgumentParser(
        description="Scrape panchayats for blocks already stored in the DB",
    )
    parser.add_argument(
        "seasons",
        nargs="*",
        help="Season(s) to process. If omitted with --state/--district/--block, seasons are inferred from those filters.",
    )
    parser.add_argument("--state", help="Filter by state_ut (case-insensitive exact match)")
    parser.add_argument("--district", help="Filter by district (case-insensitive exact match)")
    parser.add_argument("--block", help="Filter by block (case-insensitive exact match)")
    parser.add_argument(
        "--all",
        action="store_true",
        help="Re-scrape all blocks for the season, including those already marked scraped='y'",
    )
    args = parser.parse_args()

    session = get_db_session()
    engine = session.get_bind()
    try:
        if (args.block and not args.district) or (args.district and not args.state):
            parser.error("--block requires --district and --state; --district requires --state")

        seasons = args.seasons
        if not seasons and (args.state or args.district or args.block):
            metadata = MetaData()
            blocks_tbl = Table("blocks", metadata, autoload_with=engine)
            cond = []
            if args.state:
                cond.append(blocks_tbl.c.state_ut.ilike(args.state))
            if args.district:
                cond.append(blocks_tbl.c.district.ilike(args.district))
            if args.block:
                cond.append(blocks_tbl.c.block.ilike(args.block))
            stmt = select(blocks_tbl.c.season).where(and_(*cond)) if cond else select(blocks_tbl.c.season)
            stmt = stmt.distinct()
            with engine.connect() as conn:
                seasons = [r.season for r in conn.execute(stmt).fetchall()]
            if not seasons:
                logger.warning(
                    "No seasons found in DB for the given filters: state=%s, district=%s, block=%s",
                    args.state,
                    args.district,
                    args.block,
                )
                return

        if not seasons:
            logger.error(
                "No seasons provided. Provide seasons or use filters to infer seasons.",
            )
            return

        grand_total = 0
        for season in seasons:
            try:
                grand_total += run_for_season(
                    season,
                    include_all=args.all,
                    state_filter=args.state,
                    district_filter=args.district,
                    block_filter=args.block,
                )
            except Exception as e:
                logger.error("Season %s failed: %s", season, e)
        logger.info("Panchayats total upserted across seasons: %s", grand_total)
    finally:
        session.close()


if __name__ == "__main__":
    main()


