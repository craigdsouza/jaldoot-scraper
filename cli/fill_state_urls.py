import argparse
from urllib.parse import urlparse, parse_qs, quote_plus
from sqlalchemy import MetaData, Table, select, update, and_
from modules.utils import get_db_session
from config.settings import SEASON_URLS, logger


def parse_state_code_from_url(url: str) -> str | None:
    try:
        parsed = urlparse(url)
        qs = parse_qs(parsed.query)
        vals = qs.get("State")
        if vals and len(vals) > 0 and vals[0]:
            return vals[0]
    except Exception:
        pass
    return None


def build_state_code_map(engine) -> dict[str, str]:
    metadata = MetaData()
    states = Table("states", metadata, autoload_with=engine)

    stmt = select(states.c.state_ut, states.c.url).where(states.c.url.is_not(None))
    with engine.connect() as conn:
        rows = conn.execute(stmt).fetchall()

    code_map: dict[str, str] = {}
    for row in rows:
        state_ut = row.state_ut
        url = row.url
        code = parse_state_code_from_url(url)
        if code and state_ut not in code_map:
            code_map[state_ut] = code
    return code_map


def construct_url(base: str, state_code: str, state_name: str) -> str:
    return f"{base}?level=S&State={state_code}&StateName={quote_plus(state_name)}"


def fill_missing_urls(engine, state_filter: str | None, dry_run: bool) -> tuple[int, int]:
    metadata = MetaData()
    states = Table("states", metadata, autoload_with=engine)

    state_code_map = build_state_code_map(engine)

    cond = states.c.url.is_(None)
    if state_filter:
        cond = and_(cond, states.c.state_ut.ilike(state_filter))

    stmt = select(states.c.id, states.c.state_ut, states.c.season).where(cond)
    with engine.connect() as conn:
        rows = conn.execute(stmt).fetchall()

    if not rows:
        logger.info("No rows with NULL url matched the criteria.")
        return 0, 0

    updated = 0
    skipped = 0
    with engine.begin() as conn:
        for row in rows:
            state_ut = row.state_ut
            season = row.season
            base = SEASON_URLS.get(season)
            state_code = state_code_map.get(state_ut)

            if not base:
                logger.warning("No base URL configured for season %s; skipping %s", season, state_ut)
                skipped += 1
                continue
            if not state_code:
                logger.warning("No state code could be derived for %s; skipping (populate at least one season URL first)", state_ut)
                skipped += 1
                continue

            target_url = construct_url(base, state_code, state_ut.title())
            logger.info("Setting URL for %s (%s): %s", state_ut, season, target_url)
            if dry_run:
                updated += 1
                continue

            upd = (
                update(states)
                .where(states.c.id == row.id)
                .values(url=target_url)
            )
            conn.execute(upd)
            updated += 1

    return updated, skipped


def main():
    parser = argparse.ArgumentParser(
        description="Fill missing states.url using season base URLs plus required params"
    )
    parser.add_argument(
        "--state",
        metavar="STATE_UT",
        help="Only update for the specified state (case-insensitive exact match)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would change without writing to the database",
    )

    args = parser.parse_args()

    session = get_db_session()
    engine = session.get_bind()
    try:
        # SQL uses ILIKE for case-insensitive; keep exact name unless wildcard added by user
        state_filter = args.state
        updated, skipped = fill_missing_urls(engine, state_filter, args.dry_run)
        action = "would update" if args.dry_run else "updated"
        logger.info("States URLs %s: %d rows; skipped: %d", action, updated, skipped)
    finally:
        session.close()


if __name__ == "__main__":
    main()


