CREATE TABLE ballot_candidates (results_id INTEGER REFERENCES results (id) NOT NULL,
                                ballot_id INTEGER,
                                candidate_id INTEGER);
