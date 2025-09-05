-- BEGIN;
-- 
-- States
-- UPDATE states
-- SET state_ut = NULLIF(
--                   REGEXP_REPLACE(UPPER(TRIM(state_ut)), '\s+', ' ', 'g'),
--                   ''
--               );
-- 
-- Districts
-- UPDATE districts
-- SET state_ut = NULLIF(
--                   REGEXP_REPLACE(UPPER(TRIM(state_ut)), '\s+', ' ', 'g'),
--                   ''
--               ),
--     district = NULLIF(
--                   REGEXP_REPLACE(UPPER(TRIM(district)), '\s+', ' ', 'g'),
--                   ''
--               );
-- 
-- Blocks
-- UPDATE blocks
-- SET state_ut = NULLIF(
--                   REGEXP_REPLACE(UPPER(TRIM(state_ut)), '\s+', ' ', 'g'),
--                   ''
--               ),
--     district = NULLIF(
--                   REGEXP_REPLACE(UPPER(TRIM(district)), '\s+', ' ', 'g'),
--                   ''
--               ),
--     block = NULLIF(
--                   REGEXP_REPLACE(UPPER(TRIM(block)), '\s+', ' ', 'g'),
--                   ''
--               );
-- 
-- COMMIT;

-----------------------------
-- FIX INDIVIDUAL STATE NAMES
-----------------------------
-- BEGIN;
-- 
-- UPDATE states
-- SET state_ut = 'ANDAMAN AND NICOBAR'
-- WHERE state_ut = 'Andaman And Nicobar Islands';
-- 
-- UPDATE districts
-- SET state_ut = 'ANDAMAN AND NICOBAR'
-- WHERE state_ut = 'Andaman And Nicobar Islands';
-- 
-- UPDATE blocks
-- SET state_ut = 'ANDAMAN AND NICOBAR'
-- WHERE state_ut = 'Andaman And Nicobar Islands';
-- 
-- COMMIT;

-----------------------------
-- FIX INDIVIDUAL DISTRICT NAMES
-----------------------------
BEGIN;

UPDATE districts
SET district = 'SOUTH ANDAMAN'
WHERE district = 'SOUTH ANDAMANS';

UPDATE blocks
SET district = 'SOUTH ANDAMAN'
WHERE district = 'SOUTH ANDAMANS';

COMMIT;

