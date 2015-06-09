CREATE TABLE elections (id INTEGER NOT NULL,
                        results_id INTEGER REFERENCES results (id) NOT NULL,
                        PRIMARY KEY (results_id, id),
                        absentee_ballot_info TEXT,
                        absentee_request_deadline DATE,
                        date DATE,
                        election_day_registration BOOLEAN NOT NULL,
                        election_type VARCHAR(20),
                        polling_hours TEXT,
                        registration_deadline DATE,
                        registration_info TEXT,
                        results_url TEXT,
                        state_id INT,
                        statewide BOOLEAN NOT NULL);