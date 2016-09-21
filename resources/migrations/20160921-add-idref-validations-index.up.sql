create index xml_tree_values_idref_validation_idx on xml_tree_values(results_id, simple_path) where simple_path ~ '*{2}.id';
