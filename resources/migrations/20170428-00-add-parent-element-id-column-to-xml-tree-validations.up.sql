alter table xml_tree_validations add column parent_element_id text;

with values as
(SELECT v.severity, v.scope, v.path, x.value AS identifier,
        v.error_type, v.error_data
 FROM xml_tree_validations v
 LEFT JOIN xml_tree_values x
        ON x.path = subpath(v.path,0,4) || 'id' AND x.results_id = v.results_id
 INNER JOIN results r ON r.id = v.results_id)

update xml_tree_validations v
set parent_element_id = values.identifier
from values
where v.path = values.path;
