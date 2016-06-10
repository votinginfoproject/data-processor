ALTER TABLE v5_1_voter_services
      DROP COLUMN department_id,
      DROP COLUMN contact_information_id;

ALTER TABLE v5_1_departments
      DROP COLUMN department_name,
      DROP COLUMN election_administration_id,
      DROP COLUMN contact_information_id;
