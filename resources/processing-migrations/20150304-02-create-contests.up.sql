CREATE TABLE contests (id INTEGER PRIMARY KEY,
                       election_id INTEGER,
                       electoral_district_id INTEGER,
                       type TEXT,
                       partisan BOOLEAN NOT NULL CHECK (partisan IN (0,1)),
                       primary_party TEXT,
                       electorate_specifications TEXT,
                       special BOOLEAN NOT NULL CHECK (special IN (0,1)),
                       office TEXT,
                       filing_closed_date DATE,
                       number_elected INTEGER,
                       number_voting_for INTEGER,
                       ballot_id INTEGER,
                       ballot_placement INTEGER);



