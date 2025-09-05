ALTER TABLE districts
ADD CONSTRAINT districts_unique_triplet
UNIQUE (state_ut, district, season);

ALTER TABLE blocks
ADD CONSTRAINT blocks_unique_quadruplet
UNIQUE (state_ut, district, block, season);