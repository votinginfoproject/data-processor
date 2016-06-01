ALTER TABLE v5_1_contact_information
ADD COLUMN address_line TEXT,
DROP COLUMN address_line_1,
DROP COLUMN address_line_2,
DROP COLUMN address_line_3,
DROP COLUMN latitude,
DROP COLUMN longitude,
DROP COLUMN latlng_source,
ADD COLUMN phone1 TEXT,
ADD COLUMN phone2 TEXT,
DROP COLUMN phone,
DROP COLUMN parent_id;
