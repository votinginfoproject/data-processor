CREATE TABLE custom_ballot_ballot_responses (results_id BIGINT REFERENCES results (id) NOT NULL,
                                             custom_ballot_id BIGINT,
                                             ballot_response_id BIGINT,
                                             sort_order INTEGER);
