CREATE TABLE ballots (id INTEGER NOT NULL,
                      results_id INTEGER REFERENCES results (id) NOT NULL,
                      PRIMARY KEY (results_id, id),
                      referendum_id INTEGER,
                      custom_ballot_id INTEGER,
                      write_in BOOLEAN,
                      image_url TEXT);
