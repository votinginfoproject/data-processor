CREATE TABLE precincts(id BIGINT NOT NULL,
                       results_id BIGINT REFERENCES results (id) NOT NULL,
                       PRIMARY KEY (results_id, id),
                       name TEXT,
                       number TEXT,
                       locality_id BIGINT,
                       ward TEXT,
                       mail_only BOOLEAN,
                       ballot_style_image_url TEXT);
