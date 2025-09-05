from sqlalchemy import MetaData, Table
from sqlalchemy.dialects.postgresql import insert


def upsert_states(engine, df_states):
    """UPSERT rows into states on (state_ut, season). Returns affected rowcount."""
    if df_states.empty:
        return 0

    # SQLAlchemy 2.0: do not bind metadata; pass engine via autoload_with
    metadata = MetaData()
    states = Table("states", metadata, autoload_with=engine)

    rows = df_states.to_dict(orient="records")
    stmt = insert(states).values(rows)

    conflict_cols = ["state_ut", "season"]
    # Exclude immutable cols
    excluded_set = {}
    for c in states.c.keys():
        if c in ("id", "created_at") or c in conflict_cols:
            continue
        excluded_set[c] = stmt.excluded[c]

    upsert_stmt = stmt.on_conflict_do_update(index_elements=conflict_cols, set_=excluded_set)
    with engine.begin() as conn:
        result = conn.execute(upsert_stmt)
    return result.rowcount


def upsert_districts(engine, df_districts):
    """UPSERT rows into districts on (state_ut, district, season)."""
    if df_districts.empty:
        return 0

    metadata = MetaData()
    districts = Table("districts", metadata, autoload_with=engine)

    rows = df_districts.to_dict(orient="records")
    stmt = insert(districts).values(rows)

    conflict_cols = ["state_ut", "district", "season"]

    excluded_set = {}
    for c in districts.c.keys():
        if c in ("id", "created_at") or c in conflict_cols:
            continue
        excluded_set[c] = stmt.excluded[c]

    upsert_stmt = stmt.on_conflict_do_update(index_elements=conflict_cols, set_=excluded_set)
    with engine.begin() as conn:
        result = conn.execute(upsert_stmt)
    return result.rowcount


def upsert_blocks(engine, df_blocks):
    """UPSERT rows into blocks on (state_ut, district, block, season)."""
    if df_blocks.empty:
        return 0

    metadata = MetaData()
    blocks = Table("blocks", metadata, autoload_with=engine)

    rows = df_blocks.to_dict(orient="records")
    stmt = insert(blocks).values(rows)

    conflict_cols = ["state_ut", "district", "block", "season"]

    excluded_set = {}
    for c in blocks.c.keys():
        if c in ("id", "created_at") or c in conflict_cols:
            continue
        excluded_set[c] = stmt.excluded[c]

    upsert_stmt = stmt.on_conflict_do_update(index_elements=conflict_cols, set_=excluded_set)
    with engine.begin() as conn:
        result = conn.execute(upsert_stmt)
    return result.rowcount



def upsert_panchayats(engine, df_panchayats):
    """UPSERT rows into panchayats on (state_ut, district, block, panchayat, village, well_id)."""
    if df_panchayats.empty:
        return 0

    metadata = MetaData()
    panchayats = Table("panchayats", metadata, autoload_with=engine)

    rows = df_panchayats.to_dict(orient="records")
    stmt = insert(panchayats).values(rows)

    conflict_cols = [
        "state_ut",
        "district",
        "block",
        "panchayat",
        "village",
        "well_id",
    ]

    excluded_set = {}
    for c in panchayats.c.keys():
        if c in ("id", "created_at") or c in conflict_cols:
            continue
        excluded_set[c] = stmt.excluded[c]

    # Since panchayats now contains only key columns, there may be no columns to update.
    # In that case, switch to DO NOTHING on conflict.
    if not excluded_set:
        upsert_stmt = stmt.on_conflict_do_nothing(index_elements=conflict_cols)
    else:
        upsert_stmt = stmt.on_conflict_do_update(index_elements=conflict_cols, set_=excluded_set)
    with engine.begin() as conn:
        result = conn.execute(upsert_stmt)
    return result.rowcount


def upsert_well_measurements(engine, df_wells):
    """UPSERT rows into well_measurements on
    (state_ut, district, block, panchayat, village, well_id, season, year)."""
    if df_wells.empty:
        return 0

    metadata = MetaData()
    wells = Table("well_measurements", metadata, autoload_with=engine)

    rows = df_wells.to_dict(orient="records")
    stmt = insert(wells).values(rows)

    conflict_cols = [
        "state_ut",
        "district",
        "block",
        "panchayat",
        "village",
        "well_id",
        "season",
        "year",
    ]

    excluded_set = {}
    for c in wells.c.keys():
        if c in ("id", "created_at") or c in conflict_cols:
            continue
        excluded_set[c] = stmt.excluded[c]

    upsert_stmt = stmt.on_conflict_do_update(index_elements=conflict_cols, set_=excluded_set)
    with engine.begin() as conn:
        result = conn.execute(upsert_stmt)
    return result.rowcount
