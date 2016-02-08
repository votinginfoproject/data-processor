CREATE EXTENSION IF NOT EXISTS ltree;

CREATE TABLE xml_tree_values (results_id BIGINT REFERENCES results (id) NOT NULL,
                              path ltree NOT NULL,
                              value TEXT,
                              parent_with_id ltree);
