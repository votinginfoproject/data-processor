CREATE TABLE states(id BIGINT NOT NULL,
                    results_id BIGINT REFERENCES results (id) NOT NULL,
                    PRIMARY KEY (results_id, id),
                    name TEXT,
                    election_administration_id BIGINT);
