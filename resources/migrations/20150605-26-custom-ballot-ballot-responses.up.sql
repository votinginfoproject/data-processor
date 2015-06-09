CREATE TABLE custom_ballot_ballot_responses (results_id INTEGER REFERENCES results (id) NOT NULL,
                                             custom_ballot_id INTEGER,
                                             ballot_response_id INTEGER,
                                             sort_order INTEGER);
