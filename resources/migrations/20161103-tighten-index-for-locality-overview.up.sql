create index xml_tree_values_results_value_simple_path_idx
on xml_tree_values (results_id, value, simple_path)
where simple_path in ('VipObject.Precinct.LocalityId', 'VipObject.Locality.id', 'VipObject.Locality.Name');
