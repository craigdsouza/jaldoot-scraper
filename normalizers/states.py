EXPECTED_STATE_COLUMNS = [
    "state_ut",
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


def normalize_states(df_raw, season_def, season):
    """Normalize a raw states DataFrame with original site headers into
    the stable schema used by the database. Missing optional columns
    are simply omitted.
    """
    if df_raw.empty:
        return df_raw

    # Drop the leading serial column if present
    if df_raw.columns.size and df_raw.columns[0].strip() in ["#", "Sr."]:
        df_raw = df_raw.drop(df_raw.columns[0], axis=1)

    # Apply season-specific rename map
    df = df_raw.rename(columns=season_def.state_column_map)

    # Remove the "Total" row if requested
    if season_def.drop_total_row and "state_ut" in df.columns:
        df = df[df["state_ut"].str.lower() != "total"]

    # Attach season
    df["season"] = season

    # Retain only the columns we support (and that exist)
    present = [c for c in EXPECTED_STATE_COLUMNS if c in df.columns]
    return df[present]




