import pandas as pd
import streamlit as st
import altair as alt
from sqlalchemy import text, create_engine
from config.settings import DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME


@st.cache_resource
def get_engine():
    database_url = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    engine = create_engine(database_url)
    return engine


@st.cache_data(ttl=300)
def load_districts() -> list[str]:
    engine = get_engine()
    query = text(
        """
        SELECT DISTINCT district
        FROM public.panchayats
        WHERE district IS NOT NULL AND district <> ''
        ORDER BY district
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(query).fetchall()
    return [r[0] for r in rows]


@st.cache_data(ttl=300)
def load_blocks(district: str) -> list[str]:
    engine = get_engine()
    query = text(
        """
        SELECT DISTINCT block
        FROM public.panchayats
        WHERE district = :district AND block IS NOT NULL AND block <> ''
        ORDER BY block
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(query, {"district": district}).fetchall()
    return [r[0] for r in rows]


@st.cache_data(ttl=300)
def load_panchayats(district: str, block: str) -> list[str]:
    engine = get_engine()
    query = text(
        """
        SELECT DISTINCT panchayat
        FROM public.panchayats
        WHERE district = :district AND block = :block AND panchayat IS NOT NULL AND panchayat <> ''
        ORDER BY panchayat
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(query, {"district": district, "block": block}).fetchall()
    return [r[0] for r in rows]


@st.cache_data(ttl=300)
def load_villages(district: str, block: str, panchayat: str) -> list[str]:
    engine = get_engine()
    query = text(
        """
        SELECT DISTINCT village
        FROM public.panchayats
        WHERE district = :district AND block = :block AND panchayat = :panchayat AND village IS NOT NULL AND village <> ''
        ORDER BY village
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(query, {"district": district, "block": block, "panchayat": panchayat}).fetchall()
    return [r[0] for r in rows]


@st.cache_data(ttl=300)
def load_well_ids(district: str, block: str, panchayat: str, village: str) -> list[str]:
    engine = get_engine()
    query = text(
        """
        SELECT DISTINCT well_id
        FROM public.panchayats
        WHERE district = :district AND block = :block AND panchayat = :panchayat AND village = :village
          AND well_id IS NOT NULL AND well_id <> ''
        ORDER BY well_id
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(query, {"district": district, "block": block, "panchayat": panchayat, "village": village}).fetchall()
    return [r[0] for r in rows]


def season_year_to_date(season: str, year: str) -> pd.Timestamp | None:
    if not season or not year or not str(year).isdigit():
        return None
    season_lower = season.strip().lower()
    month_day = "05-01" if season_lower.startswith("pre") else "11-01"
    try:
        return pd.to_datetime(f"{int(year):04d}-{month_day}")
    except Exception:
        return None


@st.cache_data(ttl=300)
def load_timeseries_for_well(well_id: str, district: str, block: str, panchayat: str, village: str) -> pd.DataFrame:
    engine = get_engine()
    query = text(
        """
        SELECT season, year, water_level_ft, water_level_mts
        FROM public.well_measurements
        WHERE well_id = :well_id AND district = :district AND block = :block AND panchayat = :panchayat AND village = :village
        ORDER BY year::int NULLS LAST, season
        """
    )
    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params={"well_id": well_id, "district": district, "block": block, "panchayat": panchayat, "village": village})

    if df.empty:
        return df

    # Construct a date column from season/year and coerce the series to numeric
    df["date"] = [season_year_to_date(s, y) for s, y in zip(df["season"], df["year"])]
    df["water_level_ft"] = pd.to_numeric(df["water_level_ft"], errors="coerce")
    df["water_level_mts"] = pd.to_numeric(df["water_level_mts"], errors="coerce")
    df = df.dropna(subset=["date"]).sort_values("date")
    return df


def main():
    st.set_page_config(page_title="Jaldoot Dashboard", layout="wide")
    st.title("Well Water Levels Dashboard")

    # Sidebar selectors
    st.sidebar.header("Filters")
    districts = load_districts()
    district = st.sidebar.selectbox("District", options=districts, index=0) if districts else None

    blocks = load_blocks(district) if district else []
    block = st.sidebar.selectbox("Block", options=blocks) if blocks else None

    panchayats = load_panchayats(district, block) if (district and block) else []
    panchayat = st.sidebar.selectbox("Panchayat", options=panchayats) if panchayats else None

    villages = load_villages(district, block, panchayat) if (district and block and panchayat) else []
    village = st.sidebar.selectbox("Village", options=villages) if villages else None

    well_ids = load_well_ids(district, block, panchayat, village) if (district and block and panchayat and village) else []
    well_id = st.sidebar.selectbox("Well ID", options=well_ids) if well_ids else None

    if not district:
        st.info("Select a district to begin.")
        return

    if not (block and panchayat and village and well_id):
        st.info("Select Block, Panchayat, Village, and Well ID to view the time series.")
        return

    df = load_timeseries_for_well(well_id, district, block, panchayat, village)
    if df.empty:
        st.warning("No measurements found for the selected Well ID.")
        return

    st.subheader(f"Well ID: {well_id}")
    st.caption("Both feet and meters series are shown; points may be missing if not reported in that unit.")

    # Prepare tidy data for Altair and invert Y-axis (depth increases downward)
    tidy = df[["date", "water_level_ft", "water_level_mts"]].copy()
    tidy = tidy.melt(id_vars=["date"], var_name="series", value_name="value")
    tidy["series"] = tidy["series"].map({
        "water_level_ft": "Water Level (ft)",
        "water_level_mts": "Water Level (m)",
    })

    chart = (
        alt.Chart(tidy)
        .mark_line(point=True)
        .encode(
            x=alt.X("date:T", title="Date"),
            y=alt.Y("value:Q", title="Water Level", scale=alt.Scale(reverse=True)),
            color=alt.Color("series:N", title="Series"),
            tooltip=["date:T", "series:N", alt.Tooltip("value:Q", format=".2f")],
        )
        .properties(height=400)
    )

    st.altair_chart(chart, use_container_width=True)

    with st.expander("Show raw data"):
        st.dataframe(df.sort_values("date"), use_container_width=True)


if __name__ == "__main__":
    main()


