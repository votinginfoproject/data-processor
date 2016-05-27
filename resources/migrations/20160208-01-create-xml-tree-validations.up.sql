CREATE TABLE xml_tree_validations (results_id BIGINT REFERENCES results (id) NOT NULL,
                                   path ltree,
                                   severity character varying(255),
                                   scope character varying(255),
                                   error_type character varying(255),
                                   error_data text);

CREATE INDEX xml_tree_validations_result_path_idx ON xml_tree_validations (results_id, path);
CREATE INDEX xml_tree_validations_result_type_idx ON xml_tree_validations (results_id, error_type);
