CREATE TABLE electoral_districts (id BIGINT NOT NULL,
                                  results_id BIGINT REFERENCES results (id) NOT NULL,
                                  PRIMARY KEY (results_id, id),
                                  name TEXT,
                                  type TEXT,
                                  number TEXT);
