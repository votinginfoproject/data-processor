CREATE TABLE ballots (id INTEGER PRIMARY KEY,
                      referendum_id INTEGER,
                      custom_ballot_id INTEGER,
                      write_in BOOLEAN CHECK (write_in IN (0,1)),
                      image_url TEXT);
