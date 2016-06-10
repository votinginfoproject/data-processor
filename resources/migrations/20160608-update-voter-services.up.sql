ALTER TABLE v5_1_voter_services ADD COLUMN parent_id TEXT;
ALTER TABLE v5_1_voter_services RENAME COLUMN voter_service_type TO type;
