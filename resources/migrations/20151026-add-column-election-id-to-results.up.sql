ALTER TABLE results
ADD COLUMN election_id character varying(255)
              REFERENCES election_approvals(election_id);
