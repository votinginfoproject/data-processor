CREATE TABLE ballot_responses (id INTEGER NOT NULL,
                               results_id INTEGER REFERENCES results (id) NOT NULL,
                               PRIMARY KEY (results_id, id),
                               text TEXT,
                               sort_order INTEGER);
