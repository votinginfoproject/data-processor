CREATE TABLE precinct_early_vote_sites (results_id INTEGER REFERENCES results (id) NOT NULL,
                                        precinct_id INTEGER,
                                        early_vote_site_id INTEGER);
