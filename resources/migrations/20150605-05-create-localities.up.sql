CREATE TABLE localities (id BIGINT NOT NULL,
                         results_id BIGINT REFERENCES results (id) NOT NULL,
                         PRIMARY KEY (results_id, id),
                         name TEXT,
                         state_id BIGINT,
                         type TEXT,
                         election_administration_id BIGINT);
