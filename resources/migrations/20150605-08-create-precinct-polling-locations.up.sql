CREATE TABLE precinct_polling_locations(results_id INTEGER REFERENCES results (id) NOT NULL,
                                        precinct_id INTEGER,
                                        polling_location_id INTEGER);
