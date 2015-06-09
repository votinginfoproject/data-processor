CREATE TABLE localities (id INTEGER NOT NULL,
                         results_id INTEGER REFERENCES results (id) NOT NULL,
                         PRIMARY KEY (results_id, id),
                         name TEXT,
                         state_id INTEGER,
                         type TEXT,
                         election_administration_id INTEGER);
