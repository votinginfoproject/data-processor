CREATE TABLE precinct_split_polling_locations (results_id BIGINT REFERENCES results (id) NOT NULL,
                                               precinct_split_id BIGINT,
                                               polling_location_id BIGINT);
