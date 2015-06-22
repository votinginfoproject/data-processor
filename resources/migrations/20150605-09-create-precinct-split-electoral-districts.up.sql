CREATE TABLE precinct_split_electoral_districts(results_id BIGINT REFERENCES results (id) NOT NULL,
                                                precinct_split_id BIGINT,
                                                electoral_district_id BIGINT);
