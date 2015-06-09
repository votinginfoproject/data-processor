CREATE TABLE election_officials (id INTEGER NOT NULL,
                                 results_id INTEGER REFERENCES results (id) NOT NULL,
                                 PRIMARY KEY (results_id, id),
                                 name TEXT,
                                 title TEXT,
                                 phone TEXT,
                                 fax TEXT,
                                 email TEXT);
