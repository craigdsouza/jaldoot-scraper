ALTER TABLE states ADD COLUMN id SERIAL PRIMARY KEY;
ALTER TABLE districts ADD COLUMN id SERIAL PRIMARY KEY;
ALTER TABLE blocks ADD COLUMN id SERIAL PRIMARY KEY;
ALTER TABLE panchayats ADD COLUMN id SERIAL PRIMARY KEY;

-- ALTER TABLE states ADD PRIMARY KEY (state_name);
-- ALTER TABLE districts ADD PRIMARY KEY (district_name, state_id);
-- ALTER TABLE blocks ADD PRIMARY KEY (block_name, district_id);
-- ALTER TABLE panchayats ADD PRIMARY KEY (panchayat_name, block_id);