create or replace function v5_dashboard.precincts_by_locality(rid integer, locality_id text)
returns ltree[] as $$

select array_agg(xtv.parent_with_id)
  from xml_tree_values xtv
  where xtv.results_id = rid
    and xtv.simple_path = 'VipObject.Precinct.LocalityId'
    and xtv.value = locality_id
    and xtv.parent_with_id is not null;
$$ language sql;
