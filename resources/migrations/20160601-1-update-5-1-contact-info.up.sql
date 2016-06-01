ALTER TABLE v5_1_contact_information
DROP COLUMN address_line,
ADD COLUMN address_line_1 TEXT,
ADD COLUMN address_line_2 TEXT,
ADD COLUMN address_line_3 TEXT,
ADD COLUMN latitude TEXT,
ADD COLUMN longitude TEXT,
ADD COLUMN latlng_source TEXT,
DROP COLUMN phone1,
DROP COLUMN phone2,
ADD COLUMN phone TEXT,
ADD COLUMN parent_id TEXT;
