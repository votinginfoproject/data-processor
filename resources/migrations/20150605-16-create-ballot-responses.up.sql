CREATE TABLE ballot_responses (id BIGINT NOT NULL,
                               results_id BIGINT REFERENCES results (id) NOT NULL,
                               PRIMARY KEY (results_id, id),
                               text TEXT,
                               sort_order INTEGER);
