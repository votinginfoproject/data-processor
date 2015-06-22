CREATE TABLE precinct_electoral_districts (results_id BIGINT REFERENCES results (id) NOT NULL,
                                           precinct_id BIGINT,
                                           electoral_district_id BIGINT);
