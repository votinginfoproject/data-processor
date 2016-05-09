CREATE TABLE v5_0_sources (id TEXT NOT NULL,
                           results_id BIGINT REFERENCES results (id) NOT NULL,
                           PRIMARY KEY (results_id, id),
                           name TEXT,
                           vip_id TEXT,
                           date_time TEXT,
                           description TEXT,
                           organization_uri TEXT,
                           feed_contact_information_id TEXT,
                           terms_of_use_uri TEXT,
                           version TEXT);

CREATE TABLE v5_0_elections (id TEXT NOT NULL,
                             results_id BIGINT REFERENCES results (id) NOT NULL,
                             PRIMARY KEY (results_id, id),
                             date TEXT,
                             name TEXT,
                             election_type TEXT,
                             state_id TEXT,
                             is_statewide TEXT,
                             registration_info TEXT,
                             absentee_ballot_info TEXT,
                             results_uri TEXT,
                             polling_hours TEXT,
                             has_election_day_registration TEXT,
                             registration_deadline TEXT,
                             absentee_request_deadline TEXT,
                             hours_open_id TEXT);

CREATE TABLE v5_0_states (id TEXT NOT NULL,
                          results_id BIGINT REFERENCES results (id) NOT NULL,
                          PRIMARY KEY (results_id, id),
                          name TEXT,
                          election_administration_id TEXT,
                          external_identifiers TEXT);

CREATE TABLE v5_0_hours_open (id TEXT NOT NULL,
                              results_id BIGINT REFERENCES results (id) NOT NULL,
                              PRIMARY KEY (results_id, id),
                              start_time TEXT,
                              end_time TEXT,
                              is_only_by_appointment TEXT,
                              is_or_by_appointment TEXT,
                              is_subject_to_change TEXT,
                              start_date TEXT,
                              end_date TEXT);

CREATE TABLE v5_0_localities (id TEXT NOT NULL,
                              results_id BIGINT REFERENCES results (id) NOT NULL,
                              PRIMARY KEY (results_id, id),
                              name TEXT,
                              state_id TEXT,
                              type TEXT,
                              election_administration_id TEXT,
                              external_identifiers TEXT,
                              other_type TEXT,
                              polling_location_id TEXT); -- unbounded

CREATE TABLE v5_0_parties (id TEXT NOT NULL,
                           results_id BIGINT REFERENCES results (id) NOT NULL,
                           PRIMARY KEY (results_id, id),
                           abbreviation TEXT,
                           color TEXT,
                           external_identifiers TEXT,
                           logo_uri TEXT,
                           name TEXT);

CREATE TABLE v5_0_people (id TEXT NOT NULL,
                          results_id BIGINT REFERENCES results (id) NOT NULL,
                          PRIMARY KEY (results_id, id),
                          contact_information_id TEXT, -- unbounded
                          date_of_birth TEXT,
                          first_name TEXT,
                          last_name TEXT,
                          middle_name TEXT, -- unbounded
                          nickname TEXT,
                          prefix TEXT,
                          suffix TEXT,
                          title TEXT,
                          profession TEXT,
                          party_id TEXT);

CREATE TABLE v5_0_polling_locations (id TEXT NOT NULL,
                                     results_id BIGINT REFERENCES results (id) NOT NULL,
                                     PRIMARY KEY (results_id, id),
                                     address_line TEXT,
                                     directions TEXT, -- internationalized
                                     hours TEXT, -- internationalized
                                     photo_uri TEXT,
                                     hours_open_id TEXT,
                                     is_drop_box TEXT,
                                     is_early_voting TEXT,
                                     latitude TEXT,
                                     longitude TEXT,
                                     latlng_source TEXT);

CREATE TABLE v5_0_precincts (id TEXT NOT NULL,
                             results_id BIGINT REFERENCES results (id) NOT NULL,
                             PRIMARY KEY (results_id, id),
                             name TEXT,
                             number TEXT,
                             locality_id TEXT,
                             ward TEXT,
                             is_mail_only TEXT,
                             external_identifiers TEXT,
                             precinct_split_name TEXT,
                             ballot_style_id TEXT);

CREATE TABLE v5_0_election_administrations (id TEXT NOT NULL,
                                            results_id BIGINT REFERENCES results (id) NOT NULL,
                                            PRIMARY KEY (results_id, id),
                                            absentee_uri TEXT,
                                            am_i_registered_uri TEXT,
                                            elections_uri TEXT,
                                            registration_uri TEXT,
                                            rules_uri TEXT,
                                            what_is_on_my_ballot_uri TEXT,
                                            where_do_i_vote_uri TEXT);

CREATE TABLE v5_0_departments (id TEXT NOT NULL,
                               results_id BIGINT REFERENCES results (id) NOT NULL,
                               PRIMARY KEY (results_id, id),
                               election_administration_id TEXT,
                               contact_information_id TEXT,
                               election_official_person_id TEXT);

CREATE TABLE v5_0_voter_services (id TEXT NOT NULL,
                                  results_id BIGINT REFERENCES results (id) NOT NULL,
                                  PRIMARY KEY (results_id, id),
                                  department_id TEXT,
                                  contact_information_id TEXT,
                                  description TEXT, -- internationalized
                                  election_official_person_id TEXT,
                                  voter_service_type TEXT,
                                  other_type TEXT);

CREATE TABLE v5_0_electoral_districts (id TEXT NOT NULL,
                                       results_id BIGINT REFERENCES results (id) NOT NULL,
                                       PRIMARY KEY (results_id, id),
                                       name TEXT,
                                       type TEXT,
                                       number TEXT,
                                       external_identifiers TEXT,
                                       other_type TEXT);


CREATE TABLE v5_0_ballot_measure_contests (id TEXT NOT NULL,
                                           results_id BIGINT REFERENCES results (id) NOT NULL,
                                           PRIMARY KEY (results_id, id),
                                           con_statement TEXT, -- internationalized
                                           effect_of_abstain TEXT, -- internationalized
                                           full_text TEXT, -- internationalized
                                           info_uri TEXT,
                                           passage_threshold TEXT, -- internationalized
                                           pro_statement TEXT, -- internationalized
                                           summary_text TEXT, -- internationalized
                                           type TEXT,
                                           other_type TEXT);

-- Is this mixed with *candidate_selection* perhaps?
CREATE TABLE v5_0_ballot_selections (id TEXT NOT NULL,
                                     results_id BIGINT REFERENCES results (id) NOT NULL,
                                     PRIMARY KEY (results_id, id),
                                     ballot_measure_contest_id TEXT,
                                     ballot_measure_contest_selection_id TEXT,
                                     text TEXT,
                                     candidate_id TEXT,
                                     endorsement_party_id TEXT,
                                     is_write_in TEXT);

CREATE TABLE v5_0_ballot_styles (id TEXT NOT NULL,
                                 results_id BIGINT REFERENCES results (id) NOT NULL,
                                 PRIMARY KEY (results_id, id),
                                 image_uri TEXT,
                                 ordered_contest_id TEXT,
                                 party_id TEXT);

CREATE TABLE v5_0_candidates (id TEXT NOT NULL,
                              results_id BIGINT REFERENCES results (id) NOT NULL,
                              PRIMARY KEY (results_id, id),
                              ballot_name TEXT, -- internationalized
                              external_identifiers TEXT,
                              file_date TEXT,
                              is_incumbent TEXT,
                              is_top_ticket TEXT,
                              party_id TEXT,
                              person_id TEXT,
                              post_election_status TEXT,
                              pre_election_status TEXT,
                              sequence_order TEXT,
                              contest_id TEXT);

CREATE TABLE v5_0_offices (id TEXT NOT NULL,
                           results_id BIGINT REFERENCES results (id) NOT NULL,
                           PRIMARY KEY (results_id, id),
                           contact_information_id TEXT,
                           electoral_district_id TEXT,
                           external_identifiers TEXT,
                           filing_deadline TEXT,
                           is_partisan TEXT,
                           name TEXT, -- internationalized
                           office_holder_person_id TEXT,
                           term_type TEXT,
                           term_start_date TEXT,
                           term_end_date TEXT);

CREATE TABLE v5_0_candiate_contests (id TEXT NOT NULL,
                                     results_id BIGINT REFERENCES results (id) NOT NULL,
                                     PRIMARY KEY (results_id, id),
                                     abbreviation TEXT,
                                     ballot_selection_id TEXT, -- unbounded
                                     ballot_sub_title TEXT, -- internationalized
                                     ballot_title TEXT, -- internationalized
                                     electoral_district_id TEXT,
                                     electorate_specification TEXT, -- internationalized
                                     external_identifiers TEXT, -- unbounded
                                     has_rotation TEXT,
                                     name TEXT,
                                     sequence_order TEXT,
                                     vote_variation TEXT,
                                     other_vote_variation TEXT,
                                     number_elected TEXT,
                                     primary_party_id TEXT,
                                     votes_allowed TEXT,
                                     office_id TEXT); -- unbounded

CREATE TABLE v5_0_locality_polling_locations (locality_id TEXT NOT NULL,
                                              polling_location_id TEXT NOT NULL
                                              results_id BIGINT REFERENCES results (id) NOT NULL,
                                              PRIMARY KEY (results_id, locality_id, polling_location_id));

CREATE TABLE v5_0_precinct_electoral_districts (precinct_id TEXT NOT NULL,
                                                electoral_district_id TEXT NOT NULL,
                                                results_id BIGINT REFERENCES results (id) NOT NULL,
                                                PRIMARY KEY (results_id, precint_id, electoral_district_id));

CREATE TABLE v5_0_precinct_polling_locations (precinct_id TEXT NOT NULL,
                                              polling_location_id TEXT NOT NULL,
                                              results_id BIGINT REFERENCES results (id) NOT NULL,
                                              PRIMARY KEY (results_id, precint_id, polling_location_id));

CREATE TABLE v5_0_state_polling_locations (state_id TEXT NOT NULL,
                                           polling_location_id TEXT NOT NULL,
                                           results_id BIGINT REFERENCES results (id) NOT NULL,
                                           PRIMARY KEY (results_id, state_id, polling_location_id));

-- This might be adding =unit_number= only?
-- CREATE TABLE v5_0_street_segments (id TEXT NOT NULL,
--                                    results_id BIGINT REFERENCES results (id) NOT NULL,
--                                    PRIMARY KEY (results_id, id),
--                                    start_house_number TEXT,
--                                    end_house_number TEXT,
--                                    odd_even_both TEXT,
--                                    includes_all_addresses TEXT,
--                                    unit_number TEXT,
--                                    street_direction TEXT,
--                                    street_name TEXT,
--                                    street_suffix TEXT,
--                                    address_direction TEXT,
--                                    city TEXT,
--                                    state TEXT,
--                                    zip TEXT,
--                                    precinct_id TEXT);
