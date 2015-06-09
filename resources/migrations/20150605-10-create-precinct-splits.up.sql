CREATE TABLE precinct_splits(id INTEGER NOT NULL,
                             results_id INTEGER REFERENCES results (id) NOT NULL,
                             PRIMARY KEY (results_id, id),
                             name TEXT,
                             precinct_id INTEGER,
                             ballot_style_image_url TEXT);
