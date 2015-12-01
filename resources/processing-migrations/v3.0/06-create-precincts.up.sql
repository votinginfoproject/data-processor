CREATE TABLE v3_0_precincts(id INTEGER PRIMARY KEY,
                       name TEXT,
                       number TEXT,
                       locality_id INTEGER,
                       ward TEXT,
                       mail_only BOOLEAN CHECK (mail_only IN (0,1)),
                       ballot_style_image_url TEXT);
