create or replace function
v5_dashboard.locality_election_administration(rid integer, locality_id text)
returns ltree[] as $$
select array_agg(xtv.parent_with_id)
from xml_tree_values xtv
where xtv.results_id = rid
  and xtv.simple_path = 'VipObject.ElectionAdministration.id'
  and xtv.value in (select xtv.value
                    from xml_tree_values xtv
                    where xtv.results_id = rid
                      and xtv.simple_path = 'VipObject.Locality.ElectionAdministrationId'
                      and xtv.parent_with_id in (select xtv.parent_with_id
                                                 from xml_tree_values xtv
                                                 where xtv.results_id = rid
                                                   and xtv.simple_path = 'VipObject.Locality.id'
                                                   and xtv.value = locality_id));
$$ language sql;
