CREATE TABLE referendum_ballot_responses (results_id BIGINT REFERENCES results (id) NOT NULL,
                                          referendum_id BIGINT,
                                          ballot_response_id BIGINT,
                                          sort_order INTEGER);
