CREATE EXTENSION IF NOT EXISTS ltree;

CREATE TABLE xml_tree_values (results_id BIGINT REFERENCES results (id) NOT NULL,
                              path ltree NOT NULL,
                              value TEXT,
                              parent_with_id ltree);

CREATE INDEX xml_tree_values_result_path_idx ON xml_tree_values (results_id, path);
CREATE INDEX xml_tree_values_gist_path_idx ON xml_tree_values USING GIST (path);
