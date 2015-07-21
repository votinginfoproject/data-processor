ALTER TABLE elections ALTER COLUMN election_day_registration DROP NOT NULL;
ALTER TABLE elections ALTER COLUMN statewide DROP NOT NULL;
ALTER TABLE contest_results ALTER COLUMN entire_district DROP NOT NULL;
ALTER TABLE ballot_line_results ALTER COLUMN entire_district DROP NOT NULL;
