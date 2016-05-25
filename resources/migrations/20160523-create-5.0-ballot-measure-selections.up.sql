CREATE TABLE v5_1_ballot_measure_selections (id TEXT NOT NULL,
                                             results_id BIGINT REFERENCES results (id) NOT NULL,
                                             PRIMARY KEY (results_id, id),
                                             sequence_order TEXT,
                                             selection TEXT);
