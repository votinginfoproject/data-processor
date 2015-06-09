CREATE TABLE states(id INTEGER NOT NULL,
                    results_id INTEGER REFERENCES results (id) NOT NULL,
                    PRIMARY KEY (results_id, id),
                    name TEXT,
                    election_administration_id INT);
