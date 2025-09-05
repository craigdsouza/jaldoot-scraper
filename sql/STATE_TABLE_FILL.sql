---- STATES TABLE FILL

-- EXECUTE

BEGIN;

WITH seasons(season) AS (
  VALUES
    ('pre-monsoon-2023'),
    ('post-monsoon-2023'),
    ('pre-monsoon-2024'),
    ('post-monsoon-2024'),
    ('pre-monsoon-2025')
),
states_list AS (
  SELECT DISTINCT state_ut FROM states
),
expected AS (
  SELECT s.state_ut, se.season
  FROM states_list s
  CROSS JOIN seasons se
),
missing AS (
  SELECT e.state_ut, e.season
  FROM expected e
  LEFT JOIN states st
    ON st.state_ut = e.state_ut
   AND st.season = e.season
  WHERE st.id IS NULL
)
INSERT INTO states (state_ut, season /*, scraped, created_at, etc.*/)
SELECT m.state_ut, m.season /*, 'placeholder', NOW() */
FROM missing m;

COMMIT;

