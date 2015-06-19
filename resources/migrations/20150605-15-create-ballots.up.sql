CREATE TABLE ballots (id BIGINT NOT NULL,
                      results_id BIGINT REFERENCES results (id) NOT NULL,
                      PRIMARY KEY (results_id, id),
                      referendum_id BIGINT,
                      custom_ballot_id BIGINT,
                      write_in BOOLEAN,
                      image_url TEXT);
