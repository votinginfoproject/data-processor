ALTER TABLE elections ALTER COLUMN election_day_registration SET NOT NULL;
ALTER TABLE elections ALTER COLUMN statewide SET NOT NULL;
ALTER TABLE contest_results ALTER COLUMN entire_district SET NOT NULL;
ALTER TABLE ballot_line_results ALTER COLUMN entire_district SET NOT NULL;
