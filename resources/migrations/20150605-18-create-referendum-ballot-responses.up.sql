CREATE TABLE referendum_ballot_responses (results_id INTEGER REFERENCES results (id) NOT NULL,
                                          referendum_id INTEGER,
                                          ballot_response_id INTEGER,
                                          sort_order INTEGER);
