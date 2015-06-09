CREATE TABLE precinct_split_polling_locations (results_id INTEGER REFERENCES results (id) NOT NULL,
                                               precinct_split_id INTEGER,
                                               polling_location_id INTEGER);
