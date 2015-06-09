CREATE TABLE precincts(id INTEGER NOT NULL,
                       results_id INTEGER REFERENCES results (id) NOT NULL,
                       PRIMARY KEY (results_id, id),
                       name TEXT,
                       number TEXT,
                       locality_id INTEGER,
                       ward TEXT,
                       mail_only BOOLEAN,
                       ballot_style_image_url TEXT);
