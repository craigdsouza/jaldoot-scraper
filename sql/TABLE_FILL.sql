



---- DISTRICTS TABLE FILL

BEGIN;

WITH seasons(season) AS (
  VALUES
    ('pre-monsoon-2023'),
    ('post-monsoon-2023'),
    ('pre-monsoon-2024'),
    ('post-monsoon-2024'),
    ('pre-monsoon-2025')
),
pairs AS (
  SELECT DISTINCT state_ut, district FROM districts
),
expected AS (
  SELECT p.state_ut, p.district, s.season
  FROM pairs p
  CROSS JOIN seasons s
),
missing AS (
  SELECT e.state_ut, e.district, e.season
  FROM expected e
  LEFT JOIN districts d
    ON d.state_ut = e.state_ut
   AND d.district = e.district
   AND d.season = e.season
  WHERE d.id IS NULL
)
INSERT INTO districts (state_ut, district, season, scraped, created_at)
SELECT m.state_ut, m.district, m.season, 'n', NOW()
FROM missing m;

COMMIT;
