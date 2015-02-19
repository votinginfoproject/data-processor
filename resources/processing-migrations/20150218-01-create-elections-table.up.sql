CREATE TABLE elections (id INTEGER PRIMARY KEY,
                        absentee_ballot_info TEXT,
                        absentee_request_deadline DATE,
                        date DATE,
                        election_day_registration BOOLEAN NOT NULL CHECK (statewide IN (0,1)),
                        election_type VARCHAR(20), -- should this be a reference to an election_types table?
                                                   -- Is there a known list of good values?
                        polling_hours TEXT,
                        registration_deadline DATE,
                        registration_info TEXT,
                        results_url TEXT,
                        state_id INT,
                        statewide BOOLEAN NOT NULL CHECK (statewide IN (0,1)));
