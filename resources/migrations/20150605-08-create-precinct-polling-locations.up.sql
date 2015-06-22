CREATE TABLE precinct_polling_locations(results_id BIGINT REFERENCES results (id) NOT NULL,
                                        precinct_id BIGINT,
                                        polling_location_id BIGINT);
