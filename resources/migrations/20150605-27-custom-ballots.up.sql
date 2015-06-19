CREATE TABLE custom_ballots (id BIGINT NOT NULL,
                             results_id BIGINT REFERENCES results (id) NOT NULL,
                             PRIMARY KEY (results_id, id),
                             heading TEXT);
