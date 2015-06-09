CREATE TABLE precinct_electoral_districts (results_id INTEGER REFERENCES results (id) NOT NULL,
                                           precinct_id INTEGER,
                                           electoral_district_id INTEGER);
