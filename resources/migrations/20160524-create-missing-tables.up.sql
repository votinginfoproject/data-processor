CREATE TABLE v5_1_candidate_selections (id TEXT NOT NULL,
                                        results_id BIGINT REFERENCES results (id) NOT NULL,
                                        PRIMARY KEY (results_id, id),
                                        sequence_order TEXT,
                                        candidate_ids TEXT,
                                        endorsement_party_ids TEXT,
                                        is_write_in TEXT);

CREATE TABLE v5_1_contests (id TEXT NOT NULL,
                            results_id BIGINT REFERENCES results (id) NOT NULL,
                            abbreviation TEXT,
                            ballot_selection_ids TEXT,
                            ballot_sub_title TEXT,
                            ballot_title TEXT,
                            electoral_district_id TEXT,
                            electorate_specification TEXT,
                            external_identifier_type TEXT,
                            external_identifier_othertype TEXT,
                            external_identifier_value TEXT,
                            has_rotation TEXT,
                            name TEXT,
                            sequence_order TEXT,
                            vote_variation TEXT,
                            other_vote_variation TEXT);

CREATE TABLE v5_1_ordered_contests (id TEXT NOT NULL,
                                    results_id BIGINT REFERENCES results (id) NOT NULL,
                                    PRIMARY KEY (results_id, id),
                                    contest_id TEXT,
                                    ordered_ballot_selection_ids TEXT);

CREATE TABLE v5_1_party_selections (id TEXT NOT NULL,
                                    results_id BIGINT REFERENCES results (id) NOT NULL,
                                    PRIMARY KEY (results_id, id),
                                    sequence_order TEXT,
                                    party_ids TEXT);
