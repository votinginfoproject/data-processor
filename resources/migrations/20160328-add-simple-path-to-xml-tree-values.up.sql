ALTER TABLE xml_tree_values ADD COLUMN simple_path ltree;

CREATE INDEX xml_tree_values_result_simple_path_idx ON xml_tree_values (results_id, simple_path);
