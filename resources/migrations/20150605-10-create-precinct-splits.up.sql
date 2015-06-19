CREATE TABLE precinct_splits(id BIGINT NOT NULL,
                             results_id BIGINT REFERENCES results (id) NOT NULL,
                             PRIMARY KEY (results_id, id),
                             name TEXT,
                             precinct_id BIGINT,
                             ballot_style_image_url TEXT);
