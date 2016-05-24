ALTER TABLE v5_1_candidates ADD COLUMN sequence_order TEXT;

ALTER TABLE v5_1_ballot_selections DROP COLUMN sequence_order;

ALTER TABLE v5_1_contact_information
  DROP COLUMN directions,
  DROP COLUMN lat,
  DROP COLUMN lng;

ALTER TABLE v5_1_people DROP COLUMN gender;

ALTER TABLE v5_1_offices RENAME COLUMN office_holder_person_ids TO office_holder_person_id;
ALTER TABLE v5_1_ballot_styles RENAME COLUMN ordered_contest_ids TO ordered_contest_id;

-- IDREFS -> Join table
ALTER TABLE v5_1_localities DROP COLUMN polling_location_ids;
ALTER TABLE v5_1_states DROP COLUMN polling_location_ids;

ALTER TABLE v5_1_precincts
  DROP COLUMN polling_location_ids,
  DROP COLUMN electoral_district_ids;
ALTER TABLE v5_1_states DROP COLUMN polling_location_ids;

ALTER TABLE v5_1_candidate_contests
  DROP COLUMN ballot_selection_ids,
  RENAME COLUMN office_ids TO office_id,
  RENAME COLUMN primary_party_ids TO primary_party_id;

CREATE TABLE v5_1_candidate_contest_ballot_selections
(candidate_contest_id TEXT NOT NULL,
 ballot_selection_id TEXT NOT NULL,
 results_id BIGINT REFERENCES results (id) NOT NULL,
 PRIMARY KEY (results_id, candidate_contest_id, ballot_selection_id));

CREATE TABLE v5_1_precinct_electoral_districts
(precinct_id TEXT NOT NULL,
 electoral_district_id TEXT NOT NULL,
 results_id BIGINT REFERENCES results (id) NOT NULL,
 PRIMARY KEY (results_id, precinct_id, electoral_district_id));

CREATE TABLE v5_1_precinct_polling_locations
(precinct_id TEXT NOT NULL,
 polling_location_id TEXT NOT NULL,
 results_id BIGINT REFERENCES results (id) NOT NULL,
 PRIMARY KEY (results_id, precinct_id, polling_location_id));

CREATE TABLE v5_1_locality_polling_locations
(locality_id TEXT NOT NULL,
 polling_location_id TEXT NOT NULL,
 results_id BIGINT REFERENCES results (id) NOT NULL,
 PRIMARY KEY (results_id, locality_id, polling_location_id));

CREATE TABLE v5_1_state_polling_locations
(state_id TEXT NOT NULL,
 polling_location_id TEXT NOT NULL,
 results_id BIGINT REFERENCES results (id) NOT NULL,
 PRIMARY KEY (results_id, state_id, polling_location_id));
