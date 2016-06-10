ALTER TABLE v5_1_voter_services
      ADD COLUMN department_id TEXT,
      ADD COLUMN contact_information_id TEXT;

ALTER TABLE v5_1_departments
      ADD COLUMN department_name TEXT,
      ADD COLUMN election_administration_id TEXT,
      ADD COLUMN contact_information_id TEXT;
