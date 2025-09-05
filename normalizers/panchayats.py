EXPECTED_PANCHAYAT_COLUMNS = [
    "state_ut",
    "district",
    "block",
    "panchayat",
    "village",
    "well_id",
]

# Target schema for well_measurements table
EXPECTED_WELL_MEASUREMENT_COLUMNS = [
    "state_ut",
    "district",
    "block",
    "panchayat",
    "village",
    "well_id",
    "well_name",
    "season",
    "year",
    "water_level_ft",
    "water_level_mts",
    "lat",
    "long",
    "diameter_ft",
    "diameter_mts",
]


def normalize_panchayats(df_raw, season_def, season, state_name, district_name, block_name):
    """Normalize a raw panchayats DataFrame to the minimized panchayats schema.

    Produces only the parent/location keys and well_id for the `panchayats` table.
    The detailed per-well readings are handled by `normalize_well_measurements`.
    """
    if df_raw.empty:
        return df_raw

    # Drop leading serial column if present
    if df_raw.columns.size and df_raw.columns[0].strip() in ["#", "Sr.", "S.no", "S.No", "S.No."]:
        df_raw = df_raw.drop(df_raw.columns[0], axis=1)

    header_map = {
        "States/UT's": "state_ut",
        "State": "state_ut",
        "District": "district",
        "Block": "block",
        "Panchayat": "panchayat",
        "Village": "village",
        "Well ID": "well_id",
    }

    df = df_raw.rename(columns=header_map)

    # Drop columns we don't need for the minimized panchayats table
    for extra in ["Image", "URL", "Landmark"]:
        if extra in df.columns:
            df = df.drop(columns=[extra])

    # Attach parents (in case source table omitted them)
    df["state_ut"] = df.get("state_ut", state_name) if "state_ut" in df.columns else state_name
    df["district"] = district_name
    df["block"] = block_name

    # Ensure all expected columns exist even if missing from the source (fill with empty string)
    for c in EXPECTED_PANCHAYAT_COLUMNS:
        if c not in df.columns:
            df[c] = ""

    # Drop fully empty key rows to avoid generating duplicate blank keys
    if all(k in df.columns for k in ("panchayat", "village", "well_id")):
        df = df[~((df["panchayat"] == "") & (df["village"] == "") & (df["well_id"] == ""))]

    # Deduplicate on the UPSERT key
    key_cols = [
        "state_ut",
        "district",
        "block",
        "panchayat",
        "village",
        "well_id",
    ]
    existing_keys = [k for k in key_cols if k in df.columns]
    if existing_keys:
        df = df.drop_duplicates(subset=existing_keys, keep="last")

    return df[EXPECTED_PANCHAYAT_COLUMNS]


def _parse_season_year(season: str) -> tuple[str, str]:
    # Expect formats like 'pre-monsoon-2023' or 'post-monsoon-2024'
    if not season:
        return "", ""
    parts = season.split("-")
    if len(parts) >= 2 and parts[-1].isdigit():
        return "-".join(parts[:-1]), parts[-1]
    return season, ""


def normalize_well_measurements(df_raw, season: str, state_name: str, district_name: str, block_name: str):
    """Normalize raw panchayat rows into the well_measurements schema.

    - Detects units for water level and diameter (feet vs meters) based on header text.
    - Collapses latitude/longitude variants into `lat` and `long`.
    - Splits season like 'pre-monsoon-2023' into season='pre-monsoon' and year='2023'.
    - Ignores s.no, landmark, witnesses, and image columns.
    """
    if df_raw.empty:
        return df_raw

    df = df_raw.copy()

    # Drop leading serial column if present
    if df.columns.size and df.columns[0].strip() in ["#", "Sr.", "S.no", "S.No", "S.No."]:
        df = df.drop(df.columns[0], axis=1)

    # Standardize common keys
    df = df.rename(columns={
        "States/UT's": "state_ut",
        "State": "state_ut",
        "District": "district",
        "Block": "block",
        "Panchayat": "panchayat",
        "Village": "village",
        "Well ID": "well_id",
        "Well Name": "well_name",
    })

    # Identify columns by header text (case-insensitive)
    lower_cols = {c.lower(): c for c in df.columns}
    def find_col(contains_list: list[str]):
        for lc, orig in lower_cols.items():
            if all(token in lc for token in contains_list):
                return orig
        return None

    season_name, year = _parse_season_year(season)
    season_prefix = "pre" if season_name.startswith("pre") else "post"

    # Water level column
    wl_col = None
    wl_is_feet = False
    wl_is_meter = False
    candidates = []
    if season_prefix == "pre":
        candidates = [
            find_col(["pre", "water", "level", "feet"]),
            find_col(["pre", "water", "level", "meter"]),
        ]
    else:
        candidates = [
            find_col(["post", "water", "level", "feet"]),
            find_col(["post", "water", "level", "meter"]),
        ]
    candidates = [c for c in candidates if c]
    if candidates:
        wl_col = candidates[0]
        lc = wl_col.lower()
        wl_is_feet = "feet" in lc or "ft" in lc
        wl_is_meter = "meter" in lc or "metre" in lc or "mts" in lc

    # Diameter column
    dia_col = find_col(["well", "diameter"]) if df.columns.size else None
    lc_dia = dia_col.lower() if dia_col else ""
    dia_is_feet = bool(dia_col and ("feet" in lc_dia or " ft" in lc_dia or "(ft" in lc_dia))
    # Avoid false positive from the word 'diameter' containing 'meter'
    dia_is_meter = any(token in lc_dia for token in [
        " in meter", "(in meter", " in meters", "(in meters", " in metre", "(in metre", " in metres", "(in metres"
    ])

    # Latitude/Longitude columns
    lat_col = None
    long_col = None
    lat_candidates = []
    long_candidates = []
    if season_prefix == "pre":
        lat_candidates = [find_col(["pre", "latitude"]), find_col(["latitude"]) ]
        long_candidates = [find_col(["pre", "longitude"]), find_col(["longitude"]) ]
    else:
        lat_candidates = [find_col(["post", "latitude"]), find_col(["latitude"]) ]
        long_candidates = [find_col(["post", "longitude"]), find_col(["longitude"]) ]
    lat_col = next((c for c in lat_candidates if c), None)
    long_col = next((c for c in long_candidates if c), None)

    # Compose measurement dataframe
    out = df[[c for c in [
        "state_ut", "district", "block", "panchayat", "village", "well_id", "well_name"
    ] if c in df.columns]].copy()

    # Attach parents if missing
    out["state_ut"] = out.get("state_ut", state_name) if "state_ut" in out.columns else state_name
    out["district"] = out.get("district", district_name) if "district" in out.columns else district_name
    out["block"] = out.get("block", block_name) if "block" in out.columns else block_name

    out["season"], out["year"] = season_name, year

    # Map numeric/string fields while preserving verbatim strings
    out["water_level_ft"] = df[wl_col] if wl_col and wl_is_feet else ""
    out["water_level_mts"] = df[wl_col] if wl_col and wl_is_meter else ""
    out["lat"] = df[lat_col] if lat_col in df.columns else ""
    out["long"] = df[long_col] if long_col in df.columns else ""
    out["diameter_ft"] = df[dia_col] if dia_col and dia_is_feet else ""
    out["diameter_mts"] = df[dia_col] if dia_col and dia_is_meter else ""

    # Ensure all expected columns are present
    for c in EXPECTED_WELL_MEASUREMENT_COLUMNS:
        if c not in out.columns:
            out[c] = ""

    # Drop fully empty key rows
    out = out[~((out["panchayat"] == "") & (out["village"] == "") & (out["well_id"] == ""))]

    # De-duplicate on the natural key (including season+year)
    out = out.drop_duplicates(
        subset=["state_ut", "district", "block", "panchayat", "village", "well_id", "season", "year"],
        keep="last",
    )

    return out[EXPECTED_WELL_MEASUREMENT_COLUMNS]


