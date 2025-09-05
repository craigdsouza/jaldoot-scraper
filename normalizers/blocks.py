EXPECTED_BLOCK_COLUMNS = [
    "state_ut",
    "district",
    "block",
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


def normalize_blocks(df_raw, season_def, season, state_name, district_name):
    """Normalize a raw blocks DataFrame to the stable schema.

    Args:
        df_raw: DataFrame scraped from the blocks page (original headers).
        season_def: optional SeasonDef (not currently used; reserved for future header variants).
        season: Season string.
        state_name: Parent state.
        district_name: Parent district.
    """
    if df_raw.empty:
        return df_raw

    # Drop leading serial column if present
    if df_raw.columns.size and df_raw.columns[0].strip() in ["#", "Sr."]:
        df_raw = df_raw.drop(df_raw.columns[0], axis=1)

    # Standard header map seen across seasons
    header_map = {
        "Block": "block",
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

    df = df_raw.rename(columns=header_map)

    # Remove total row
    if "block" in df.columns:
        df = df[df["block"].str.lower() != "total"]

    # Attach parents and season
    df["state_ut"] = state_name
    df["district"] = district_name
    df["season"] = season

    present = [c for c in EXPECTED_BLOCK_COLUMNS if c in df.columns]
    return df[present]


