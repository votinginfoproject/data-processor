CREATE TABLE election_approvals (election_id character varying(255) PRIMARY KEY,
                                 approved_result_id int REFERENCES results(id));
