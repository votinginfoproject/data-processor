CREATE TABLE precincts(id INTEGER PRIMARY KEY,
                       name TEXT,
                       number TEXT,
                       locality_id INTEGER,
                       ward TEXT,
                       mail_only BOOLEAN NOT NULL CHECK (mail_only IN (0,1)),
                       ballot_style_image_url TEXT);