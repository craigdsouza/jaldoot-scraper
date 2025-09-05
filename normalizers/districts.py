EXPECTED_DISTRICT_COLUMNS = [
    "state_ut",
    "district",
    "total_no_of_panchayat",
    "no_of_panchayat_covered",
    "no_of_village_covered",
    "no_of_well_covered",
    "no_of_wells_0_2_feet",
    "no_of_wells_3_5_feet",
    "no_of_wells_6_10_feet",
    "no_of_wells_gt_10_feet",
    "url",
    "season",
]


def normalize_districts(df_raw, season_def, season, state_name):
    """Normalize a raw districts DataFrame to the stable schema.

    Args:
        df_raw: DataFrame scraped from the districts page (original headers).
        season_def: SeasonDef with column map for districts if available (fallback to states map for overlapping fields).
        season: Season string.
        state_name: Parent state name to attach to each row.
    """
    if df_raw.empty:
        return df_raw

    # Drop leading serial column if present
    if df_raw.columns.size and df_raw.columns[0].strip() in ["#", "Sr."]:
        df_raw = df_raw.drop(df_raw.columns[0], axis=1)

    # Build a column map. District pages share many headers with states.
    base_map = {
        "District": "district",
        "Total No. of Panchayat": "total_no_of_panchayat",
        "No. of Panchayat Covered": "no_of_panchayat_covered",
        "No. of Village Covered": "no_of_village_covered",
        "No. of Well Covered": "no_of_well_covered",
        "0-2 Feet": "no_of_wells_0_2_feet",
        "3-5 Feet": "no_of_wells_3_5_feet",
        "6-10 Feet": "no_of_wells_6_10_feet",
        ">10 Feet": "no_of_wells_gt_10_feet",
        "URL": "url",
    }

    # Apply rename
    df = df_raw.rename(columns=base_map)

    # Remove the total row
    if "district" in df.columns:
        df = df[df["district"].str.lower() != "total"]

    # Attach parent keys and season
    df["state_ut"] = state_name
    df["season"] = season

    # Retain supported columns
    present = [c for c in EXPECTED_DISTRICT_COLUMNS if c in df.columns]
    return df[present]


