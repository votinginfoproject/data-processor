CREATE TABLE precinct_split_electoral_districts(results_id INTEGER REFERENCES results (id) NOT NULL,
                                                precinct_split_id INTEGER,
                                                electoral_district_id INTEGER);
