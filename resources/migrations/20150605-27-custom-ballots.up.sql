CREATE TABLE custom_ballots (id INTEGER NOT NULL,
                             results_id INTEGER REFERENCES results (id) NOT NULL,
                             PRIMARY KEY (results_id, id),
                             heading TEXT);
