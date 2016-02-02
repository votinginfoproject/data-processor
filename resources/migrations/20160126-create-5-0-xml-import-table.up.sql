CREATE EXTENSION ltree;

CREATE TABLE v5_0_xml_values (results_id BIGINT REFERENCES results (id) NOT NULL,
                              path ltree NOT NULL,
                              value TEXT,
                              parent_with_id ltree);
