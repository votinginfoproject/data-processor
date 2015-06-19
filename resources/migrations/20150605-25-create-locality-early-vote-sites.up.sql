CREATE TABLE locality_early_vote_sites (results_id BIGINT REFERENCES results (id) NOT NULL,
                                        locality_id BIGINT,
                                        early_vote_site_id BIGINT);
