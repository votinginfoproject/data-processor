CREATE TABLE ballot_candidates (results_id BIGINT REFERENCES results (id) NOT NULL,
                                ballot_id BIGINT,
                                candidate_id BIGINT);
