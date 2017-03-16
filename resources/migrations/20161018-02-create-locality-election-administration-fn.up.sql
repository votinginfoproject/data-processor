create or replace function locality_election_administration(locality_id text, rid text)
returns ltree[] as $$
select array_agg(xtv.parent_with_id)
from results r
left join xml_tree_values xtv on xtv.results_id = r.id
where r.public_id = rid
  and xtv.simple_path = 'VipObject.ElectionAdministration.id'
  and xtv.value in (select xtv.value
                    from results r
                    left join xml_tree_values xtv on xtv.results_id = r.id
                    where r.public_id = rid
                      and xtv.simple_path = 'VipObject.Locality.ElectionAdministrationId'
                      and xtv.parent_with_id in (select xtv.parent_with_id
                                                 from results r
                                                 left join xml_tree_values xtv on r.id = xtv.results_id
                                                 where r.public_id = rid
                                                   and xtv.simple_path = 'VipObject.Locality.id'
                                                   and xtv.value = locality_id));
$$ language sql;
