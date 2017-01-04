create index xtv_ss_precinct_idx
on xml_tree_values (results_id, simple_path, value)
where simple_path = 'VipObject.StreetSegment.PrecinctId';
