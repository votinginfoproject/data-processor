CREATE TABLE election_officials (id BIGINT NOT NULL,
                                 results_id BIGINT REFERENCES results (id) NOT NULL,
                                 PRIMARY KEY (results_id, id),
                                 name TEXT,
                                 title TEXT,
                                 phone TEXT,
                                 fax TEXT,
                                 email TEXT);
