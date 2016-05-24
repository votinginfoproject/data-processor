ALTER TABLE v5_1_candidates DROP COLUMN sequence_order;

ALTER TABLE v5_1_ballot_selections ADD COLUMN sequence_order TEXT;

ALTER TABLE v5_1_contact_information
  ADD COLUMN directions TEXT,
  ADD COLUMN lat TEXT,
  ADD COLUMN lng TEXT;

ALTER TABLE v5_1_people ADD COLUMN gender TEXT;

ALTER TABLE v5_1_ballot_styles RENAME COLUMN ordered_contest_id TO ordered_contest_ids;
ALTER TABLE v5_1_candidate_contests RENAME COLUMN office_id TO office_ids;
ALTER TABLE v5_1_candidate_contests RENAME COLUMN primary_party_id TO primary_party_ids;
ALTER TABLE v5_1_offices RENAME COLUMN office_holder_person_id TO office_holder_person_ids;

-- IDREF becomes IDREFS, and we can drop some join tables
ALTER TABLE v5_1_localities ADD COLUMN polling_location_ids TEXT;
ALTER TABLE v5_1_states ADD COLUMN polling_location_ids TEXT;
ALTER TABLE v5_1_candidate_contests ADD COLUMN ballot_selection_ids TEXT;
ALTER TABLE v5_1_precincts
  ADD COLUMN electoral_district_ids TEXT,
  ADD COLUMN polling_location_ids TEXT;

DROP TABLE v5_1_candidate_contest_ballot_selections;
DROP TABLE v5_1_precinct_electoral_districts;
DROP TABLE v5_1_precinct_polling_locations;
DROP TABLE v5_1_locality_polling_locations;
DROP TABLE v5_1_state_polling_locations;
