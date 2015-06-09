CREATE TABLE electoral_districts (id INTEGER NOT NULL,
                                  results_id INTEGER REFERENCES results (id) NOT NULL,
                                  PRIMARY KEY (results_id, id),
                                  name TEXT,
                                  type TEXT,
                                  number TEXT);
