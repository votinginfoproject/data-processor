CREATE TABLE state_early_vote_sites (results_id INTEGER REFERENCES results (id) NOT NULL,
                                     state_id INTEGER,
                                     early_vote_site_id INTEGER);
