CREATE TABLE sources (id INTEGER NOT NULL,
                      results_id INTEGER REFERENCES results (id) NOT NULL,
                      PRIMARY KEY (results_id, id),
                      name TEXT,
                      vip_id TEXT,
                      datetime DATE,
                      description TEXT,
                      organization_url TEXT,
                      feed_contact_id INT,
                      tou_url TEXT);