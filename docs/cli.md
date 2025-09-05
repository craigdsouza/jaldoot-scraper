# CLI Reference

This project exposes several command-line entry points for scraping and for backfilling missing URLs. Run commands from the project root using Python module syntax.

## States

Scrape and upsert state-level rows for one or more seasons.

```bash
python -m cli.states pre-monsoon-2023 post-monsoon-2023
```

- Uses season base URL from `config/season_registry.py`.
- Normalizes columns and UPSERTs into `states` on `(state_ut, season)`.

## Districts

Scrape district rows for states already present in the `states` table.

```bash
# All states for a given season
python -m cli.districts pre-monsoon-2023

# One state for a given season
python -m cli.districts pre-monsoon-2023 --state "ARUNACHAL PRADESH"

# All seasons recorded for a given state (no season args)
python -m cli.districts --state "ARUNACHAL PRADESH"

# Re-scrape rows previously marked scraped='y'
python -m cli.districts pre-monsoon-2023 --all
```

State `scraped` status in `states` is set per state/season as:
- `y`: one or more district rows found and upserted
- `e`: zero usable rows (e.g., only a Total row or truly empty table)
- `n`: an error occurred during scraping

## Blocks

Scrape block rows for districts already present in the `districts` table.

```bash
# Process all districts for one or more seasons
python -m cli.blocks pre-monsoon-2023

# One state for a given season
python -m cli.blocks pre-monsoon-2023 --state "ARUNACHAL PRADESH"

# One district for a given season
python -m cli.blocks pre-monsoon-2023 --state "ARUNACHAL PRADESH" --district "ANJAW"

# All seasons recorded for a given state/district (no season args)
python -m cli.blocks --state "ARUNACHAL PRADESH"
python -m cli.blocks --district "ANJAW"

# Re-scrape even if previously marked scraped='y'
python -m cli.blocks pre-monsoon-2023 --all
```

- Normalizes and UPSERTs into `blocks` on `(state_ut, district, block, season)`.
- When `--state`/`--district` are provided without seasons, the CLI infers seasons from existing `districts` rows matching the filters.
- When using `--district`, you must also specify `--state` because some district names repeat across states.
- District `scraped` status in `districts` is set per district/season as:
  - `y`: one or more block rows found and upserted
  - `e`: zero usable rows (e.g., only a Total row or truly empty table)
  - `n`: an error occurred during scraping

Block scrape progress:
- `blocks.scraped` remains a status flag (`y`/`e`/`n`).
- `blocks.scraped_count` (INTEGER) records the number of well records captured for the block/season by the panchayats scraper.
  - You can add this column via:
    ```sql
    ALTER TABLE public.blocks ADD COLUMN IF NOT EXISTS scraped_count INTEGER DEFAULT 0;
    ```

## Panchayats

Scrape panchayat-level rows (well records) for blocks already present in the `blocks` table.

```bash
# Process all blocks for one or more seasons
python -m cli.panchayats pre-monsoon-2023

# One state for a given season
python -m cli.panchayats pre-monsoon-2023 --state "ARUNACHAL PRADESH"

# One district for a given season
python -m cli.panchayats pre-monsoon-2023 --state "ARUNACHAL PRADESH" --district "ANJAW"

# One block for a given season
python -m cli.panchayats pre-monsoon-2023 --state "ARUNACHAL PRADESH" --district "ANJAW" --block "ANJAW BLOCK"

# All seasons recorded for a given filter set (no season args)
python -m cli.panchayats --state "ARUNACHAL PRADESH"
python -m cli.panchayats --state "ARUNACHAL PRADESH" --district "ANJAW"
python -m cli.panchayats --state "ARUNACHAL PRADESH" --district "ANJAW" --block "ANJAW BLOCK"

# Re-scrape even if previously marked scraped='y' on blocks
python -m cli.panchayats pre-monsoon-2023 --all
```

- Normalizes and UPSERTs into `panchayats` on `(state_ut, district, block, panchayat, village, well_id)` (keys only).
- Normalizes and UPSERTs detailed readings into `well_measurements` on `(state_ut, district, block, panchayat, village, well_id, season, year)`.
- When using `--block`, you must also specify `--district` and `--state`.
- Block scrape result updates (`blocks`):
  - On success: `scraped='y'` and `scraped_count` set to the number of `well_measurements` rows upserted for that block/season.
  - On empty table or normalization produced zero rows: `scraped='e'`, `scraped_count=0`.
  - On scraping error: `scraped='n'`, `scraped_count=0`.

Notes on parsing and units:
- The scraper auto-detects table ids (e.g., `example`, `ContentPlaceHolder1_gvReport`) and header structures (`thead` vs `tr.header`).
- `well_measurements` stores both feet/meters variants: only the appropriate unit column is filled per season.
- `season` like `pre-monsoon-2023` is split into `season='pre-monsoon'` and `year='2023'`.

## Common CLI errors

- Missing seasons: provide at least one season, or pass a filter (e.g., `--state`) so the CLI can infer seasons from the DB.
- District without state: `--district` requires `--state` in block scraping to avoid ambiguous district names.
- Empty DB prerequisites: run the prior levels first (e.g., `states` then `districts`) so downstream CLIs have URLs to crawl.
- Headless/browser issues: ensure Chrome/Chromedriver are installed and compatible; set headless options via `.env` per `config/settings.py`.
- Database connectivity: verify `.env` DB settings and that schema from `sql/add_primary_keys.sql` is applied.

## Fill Missing State URLs

Backfill `states.url` where NULL using season base URLs and known state codes.

```bash
# Preview changes for a state
python -m cli.fill_state_urls --state "ARUNACHAL PRADESH" --dry-run

# Apply for a state
python -m cli.fill_state_urls --state "ARUNACHAL PRADESH"

# Apply globally
python -m cli.fill_state_urls
```

- Uses `SEASON_URLS` from `config/settings.py`.
- Derives `State` code by parsing any existing `states.url` rows.
- Constructs: `BASE?level=S&State=<code>&StateName=<state>`.

## Fill Missing District URLs

Backfill `districts.url` where NULL using season base URLs and known state/district codes.

```bash
# Preview changes for a state
python -m cli.fill_district_urls --state "ARUNACHAL PRADESH" --dry-run

# Apply for a state
python -m cli.fill_district_urls --state "ARUNACHAL PRADESH"

# Apply globally
python -m cli.fill_district_urls
```

- Uses `SEASON_URLS` from `config/settings.py`.
- Derives `State` code from any `states.url`; derives `District` code from any populated `districts.url` for that district.
- Constructs: `BASE?level=D&District=<code>&DistrictName=<name>&State=<code>&StateName=<name>`.

## Prerequisites

- Database configured and schema applied from `sql/add_primary_keys.sql`.
- `.env` configured per `config/settings.py` expectations (DB connection, headless mode, etc.).
