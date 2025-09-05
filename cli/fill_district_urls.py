import argparse
from urllib.parse import urlparse, parse_qs, quote_plus
from sqlalchemy import MetaData, Table, select, update, and_
from modules.utils import get_db_session
from config.settings import SEASON_URLS, logger


def parse_code(url: str, key: str) -> str | None:
    try:
        parsed = urlparse(url)
        qs = parse_qs(parsed.query)
        vals = qs.get(key)
        if vals and len(vals) > 0 and vals[0]:
            return vals[0]
    except Exception:
        pass
    return None


def build_state_and_district_code_maps(engine):
    metadata = MetaData()
    states_tbl = Table("states", metadata, autoload_with=engine)
    districts_tbl = Table("districts", metadata, autoload_with=engine)

    state_code_by_state: dict[str, str] = {}
    # From states.url parse State code
    stmt_states = select(states_tbl.c.state_ut, states_tbl.c.url).where(states_tbl.c.url.is_not(None))
    with engine.connect() as conn:
        for r in conn.execute(stmt_states):
            code = parse_code(r.url, "State")
            if code and r.state_ut not in state_code_by_state:
                state_code_by_state[r.state_ut] = code

    # From districts.url parse District codes per (state_ut, district)
    district_code_by_key: dict[tuple[str, str], str] = {}
    stmt_d = select(districts_tbl.c.state_ut, districts_tbl.c.district, districts_tbl.c.url).where(districts_tbl.c.url.is_not(None))
    with engine.connect() as conn:
        for r in conn.execute(stmt_d):
            code = parse_code(r.url, "District")
            key = (r.state_ut, r.district)
            if code and key not in district_code_by_key:
                district_code_by_key[key] = code

    return state_code_by_state, district_code_by_key


def construct_district_url(base: str, state_code: str, state_name: str, district_code: str, district_name: str) -> str:
    # ordering follows observed example, but query order typically does not matter
    return (
        f"{base}?level=D&District={district_code}"
        f"&DistrictName={quote_plus(district_name)}"
        f"&State={state_code}&StateName={quote_plus(state_name)}"
    )


def fill_missing_district_urls(engine, state_filter: str | None, dry_run: bool) -> tuple[int, int, int]:
    metadata = MetaData()
    districts = Table("districts", metadata, autoload_with=engine)

    state_code_by_state, district_code_by_key = build_state_and_district_code_maps(engine)

    cond = districts.c.url.is_(None)
    if state_filter:
        cond = and_(cond, districts.c.state_ut.ilike(state_filter))

    stmt = select(districts.c.id, districts.c.state_ut, districts.c.district, districts.c.season).where(cond)
    with engine.connect() as conn:
        rows = conn.execute(stmt).fetchall()

    if not rows:
        logger.info("No district rows with NULL url matched the criteria.")
        return 0, 0, 0

    updated = 0
    skipped_no_base = 0
    skipped_no_codes = 0
    with engine.begin() as conn:
        for r in rows:
            base = SEASON_URLS.get(r.season)
            if not base:
                skipped_no_base += 1
                logger.warning("No base URL for season %s; skipping %s / %s", r.season, r.state_ut, r.district)
                continue

            state_code = state_code_by_state.get(r.state_ut)
            district_code = district_code_by_key.get((r.state_ut, r.district))

            if not state_code or not district_code:
                skipped_no_codes += 1
                logger.warning("Missing codes (state=%s, district=%s) for %s / %s; skipping", state_code, district_code, r.state_ut, r.district)
                continue

            url = construct_district_url(base, state_code, r.state_ut.title(), district_code, r.district.title())
            logger.info("Setting district URL for %s / %s (%s): %s", r.state_ut, r.district, r.season, url)
            if dry_run:
                updated += 1
                continue
            upd = update(districts).where(districts.c.id == r.id).values(url=url)
            conn.execute(upd)
            updated += 1

    return updated, skipped_no_base, skipped_no_codes


def main():
    parser = argparse.ArgumentParser(description="Fill missing districts.url using season base URLs plus params")
    parser.add_argument("--state", help="Only process the specified state_ut (case-insensitive exact match)")
    parser.add_argument("--dry-run", action="store_true", help="Show what would change without writing to DB")
    args = parser.parse_args()

    session = get_db_session()
    engine = session.get_bind()
    try:
        updated, skipped_no_base, skipped_no_codes = fill_missing_district_urls(engine, args.state, args.dry_run)
        action = "would update" if args.dry_run else "updated"
        logger.info(
            "District URLs %s: %d; skipped (no base): %d; skipped (missing codes): %d",
            action, updated, skipped_no_base, skipped_no_codes,
        )
    finally:
        session.close()


if __name__ == "__main__":
    main()



