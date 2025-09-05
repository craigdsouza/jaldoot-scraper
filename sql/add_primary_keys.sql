--
-- STATES (works across seasons)
-- This schema is a superset of columns observed across seasons:
-- - Some seasons include "Total No. of Panchayat".
-- - Depth-band subcolumns may appear under either
--   "No. of Wells with Water Level" OR
--   "No. of Wells Covered with Water Level" (treated the same).
-- The scraper maps all variants into these stable column names.
--
CREATE TABLE IF NOT EXISTS states (
    id SERIAL PRIMARY KEY,
    state_ut VARCHAR(255) NOT NULL,
    total_no_of_panchayat VARCHAR(20),        -- Present in some seasons
    no_of_panchayat_covered VARCHAR(20),
    no_of_village_covered VARCHAR(20),
    no_of_well_covered VARCHAR(20),
    no_of_wells_0_2_feet VARCHAR(20),        -- Depth bands (optional in some seasons)
    no_of_wells_3_5_feet VARCHAR(20),
    no_of_wells_6_10_feet VARCHAR(20),
    no_of_wells_gt_10_feet VARCHAR(20),
    url TEXT,
    season VARCHAR(50),
    scraped VARCHAR(1),                       -- 'y' or 'n' status for downstream scrape
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Grant rights on the table and its sequence
GRANT INSERT, SELECT, UPDATE, DELETE ON TABLE public.states TO jaldoot_user;
GRANT USAGE, SELECT ON SEQUENCE public.states_id_seq TO jaldoot_user;

-- Optional but recommended: make the app user the owner
ALTER TABLE public.states OWNER TO jaldoot_user;
ALTER SEQUENCE public.states_id_seq OWNER TO jaldoot_user;
ALTER SEQUENCE public.states_id_seq OWNED BY public.states.id;






-- DISTRICTS (works across seasons)
-- Superset schema: includes optional Total No. of Panchayat and depth-band columns.
CREATE TABLE IF NOT EXISTS public.districts (
    id SERIAL PRIMARY KEY,
    state_ut VARCHAR(255) NOT NULL,
    district VARCHAR(255) NOT NULL,
    total_no_of_panchayat VARCHAR(20),        -- Present in some seasons
    no_of_panchayat_covered VARCHAR(20),
    no_of_village_covered VARCHAR(20),
    no_of_well_covered VARCHAR(20),
    no_of_wells_0_2_feet VARCHAR(20),         -- Optional depth bands
    no_of_wells_3_5_feet VARCHAR(20),
    no_of_wells_6_10_feet VARCHAR(20),
    no_of_wells_gt_10_feet VARCHAR(20),
    url TEXT,
    season VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Privileges and ownership (align with your states grants)
GRANT INSERT, SELECT, UPDATE, DELETE ON TABLE public.districts TO jaldoot_user;
GRANT USAGE, SELECT ON SEQUENCE public.districts_id_seq TO jaldoot_user;

ALTER TABLE public.districts OWNER TO jaldoot_user;
ALTER SEQUENCE public.districts_id_seq OWNER TO jaldoot_user;
ALTER SEQUENCE public.districts_id_seq OWNED BY public.districts.id;

-- Indexes
CREATE INDEX IF NOT EXISTS idx_districts_season ON public.districts(season);
CREATE INDEX IF NOT EXISTS idx_districts_state_ut ON public.districts(state_ut);

-- Idempotent UPSERT key per season
CREATE UNIQUE INDEX IF NOT EXISTS ux_districts_key
ON public.districts(state_ut, district, season);







-- BLOCKS (works across seasons)
-- Superset schema: includes optional Total No. of Panchayat and depth-band columns.
CREATE TABLE IF NOT EXISTS public.blocks (
    id SERIAL PRIMARY KEY,
    state_ut VARCHAR(255) NOT NULL,
    district VARCHAR(255) NOT NULL,
    block VARCHAR(255) NOT NULL,
    total_no_of_panchayat VARCHAR(20),
    no_of_panchayat_covered VARCHAR(20),
    no_of_village_covered VARCHAR(20),
    no_of_well_covered VARCHAR(20),
    no_of_wells_0_2_feet VARCHAR(20),
    no_of_wells_3_5_feet VARCHAR(20),
    no_of_wells_6_10_feet VARCHAR(20),
    no_of_wells_gt_10_feet VARCHAR(20),
    url TEXT,
    season VARCHAR(50),
    scraped VARCHAR(1),
    scraped_count INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Privileges and ownership (align with your states grants)
GRANT INSERT, SELECT, UPDATE, DELETE ON TABLE public.blocks TO jaldoot_user;
GRANT USAGE, SELECT ON SEQUENCE public.blocks_id_seq TO jaldoot_user;

ALTER TABLE public.blocks OWNER TO jaldoot_user;
ALTER SEQUENCE public.blocks_id_seq OWNER TO jaldoot_user;
ALTER SEQUENCE public.blocks_id_seq OWNED BY public.blocks.id;

-- Idempotent UPSERT key and helpful indexes
CREATE INDEX IF NOT EXISTS idx_blocks_season ON public.blocks(season);
CREATE INDEX IF NOT EXISTS idx_blocks_state_district ON public.blocks(state_ut, district);

-- Idempotent UPSERT key per season
CREATE UNIQUE INDEX IF NOT EXISTS ux_blocks_key
ON public.blocks(state_ut, district, block, season);






-- PANCHAYATS (leaf-level wells table)
-- Columns mirror the on-page table headers; stored as text to preserve exact values
CREATE TABLE IF NOT EXISTS public.panchayats (
    id SERIAL PRIMARY KEY,
    state_ut VARCHAR(255) NOT NULL,
    district VARCHAR(255) NOT NULL,
    block VARCHAR(255) NOT NULL,
    panchayat VARCHAR(255),
    village VARCHAR(255),
    well_id VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Privileges and ownership (align with other tables)
GRANT INSERT, SELECT, UPDATE, DELETE ON TABLE public.panchayats TO jaldoot_user;
GRANT USAGE, SELECT ON SEQUENCE public.panchayats_id_seq TO jaldoot_user;

ALTER TABLE public.panchayats OWNER TO jaldoot_user;
ALTER SEQUENCE public.panchayats_id_seq OWNER TO jaldoot_user;
ALTER SEQUENCE public.panchayats_id_seq OWNED BY public.panchayats.id;

-- Helpful indexes for filtering and querying
CREATE INDEX IF NOT EXISTS idx_panchayats_state_district_block ON public.panchayats(state_ut, district, block);

-- Idempotent UPSERT key per season
CREATE UNIQUE INDEX IF NOT EXISTS ux_panchayats_key
ON public.panchayats(state_ut, district, block, panchayat, village, well_id);






-- WELL MEASUREMENTS (normalized leaf-level wells table across seasons/years)
-- Schema captures both feet and meters variants observed across different seasons.
-- Lat/Long are stored as simple `lat`, `long` columns irrespective of source header variants.
-- The `season` value like 'pre-monsoon-2023' should be split upstream into
--   season='pre-monsoon' and year='2023' before insert.
CREATE TABLE IF NOT EXISTS public.well_measurements (
    id SERIAL PRIMARY KEY,
    state_ut VARCHAR(255) NOT NULL,
    district VARCHAR(255) NOT NULL,
    block VARCHAR(255) NOT NULL,
    panchayat VARCHAR(255),
    village VARCHAR(255),
    well_id VARCHAR(255),
    well_name VARCHAR(255),
    season VARCHAR(50) NOT NULL,
    year VARCHAR(4) NOT NULL,
    water_level_ft VARCHAR(50),
    water_level_mts VARCHAR(50),
    lat VARCHAR(50),
    long VARCHAR(50),
    diameter_ft VARCHAR(50),
    diameter_mts VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Privileges and ownership (align with other tables)
GRANT INSERT, SELECT, UPDATE, DELETE ON TABLE public.well_measurements TO jaldoot_user;
GRANT USAGE, SELECT ON SEQUENCE public.well_measurements_id_seq TO jaldoot_user;

ALTER TABLE public.well_measurements OWNER TO jaldoot_user;
ALTER SEQUENCE public.well_measurements_id_seq OWNER TO jaldoot_user;
ALTER SEQUENCE public.well_measurements_id_seq OWNED BY public.well_measurements.id;

-- Helpful indexes for filtering and querying
CREATE INDEX IF NOT EXISTS idx_well_measurements_season_year ON public.well_measurements(season, year);
CREATE INDEX IF NOT EXISTS idx_well_measurements_state_district_block ON public.well_measurements(state_ut, district, block);
CREATE INDEX IF NOT EXISTS idx_well_measurements_well_id ON public.well_measurements(well_id);

-- Idempotent UPSERT key per season/year
CREATE UNIQUE INDEX IF NOT EXISTS ux_well_measurements_key
ON public.well_measurements(state_ut, district, block, panchayat, village, well_id, season, year);
