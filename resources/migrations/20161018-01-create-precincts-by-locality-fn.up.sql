create or replace function precincts_by_locality(locality_id text, rid text)
returns ltree[] as $$

select array_agg(xtv.parent_with_id)
  from results r
  left join xml_tree_values xtv on r.id = xtv.results_id
  where r.public_id = rid
    and xtv.simple_path = 'VipObject.Precinct.LocalityId'
    and xtv.value = locality_id;
$$ language sql;
