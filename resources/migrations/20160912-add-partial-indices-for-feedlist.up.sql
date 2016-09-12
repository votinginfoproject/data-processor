-- the feed list has three seq scans on xml_tree_values and be pretty much unusable
drop index if exists xml_tree_values_results_state_path_idx;
create index xml_tree_values_results_state_path_idx on xml_tree_values(results_id, simple_path) where simple_path = 'VipObject.State.Name';

drop index if exists xml_tree_values_results_date_path_idx;
create index xml_tree_values_results_date_path_idx on xml_tree_values(results_id, simple_path) where simple_path = 'VipObject.Election.Date';

drop index if exists xml_tree_values_results_election_type_path_idx;
create index xml_tree_values_results_election_type_path_idx on xml_tree_values(results_id, path) where path ~ 'VipObject.*.Election.*.ElectionType.*.Text.0';

-- help the `approvable_status` query, as used on a feed's overview page
drop index if exists validations_results_scoped_severity_idx;
create index validations_results_scoped_severity_idx on validations(results_id, severity) where severity = 'critical' or severity = 'fatal';
