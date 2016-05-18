ALTER TABLE v5_0_ballot_measure_contests
 ADD COLUMN abbreviation TEXT,
 ADD COLUMN ballot_selection_ids TEXT,
 ADD COLUMN ballot_sub_title TEXT,
 ADD COLUMN ballot_title TEXT,
 ADD COLUMN electoral_district_id TEXT,
 ADD COLUMN electorate_specification TEXT,
 ADD COLUMN external_identifier_type TEXT,
 ADD COLUMN external_identifier_othertype TEXT,
 ADD COLUMN external_identifier_value TEXT,
 ADD COLUMN has_rotation TEXT,
 ADD COLUMN name TEXT,
 ADD COLUMN sequence_order TEXT,
 ADD COLUMN vote_variation TEXT,
 ADD COLUMN other_vote_variation TEXT;
