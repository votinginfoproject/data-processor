ALTER TABLE v5_1_voter_services DROP COLUMN parent_id;
ALTER TABLE v5_1_voter_services RENAME COLUMN type TO voter_service_type;
