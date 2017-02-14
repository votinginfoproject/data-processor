create or replace function v5_dashboard.locality_error_report(pid text, given_locality_id text)
returns table (results_id bigint, path text, severity varchar, scope varchar, identifier text, error_type varchar, error_data text)
as $$
declare results_id_from_pid int;
begin
  select id into results_id_from_pid from results where results.public_id = pid;
  return query
  select xtv.results_id, ltree2text(xtv.path), xtv.severity, xtv.scope, x.value AS identifier, xtv.error_type, xtv.error_data
  from xml_tree_validations xtv
  left join xml_tree_values x
         on x.path = subpath(xtv.path,0,4) || 'id' and x.results_id = xtv.results_id
  inner join results r on r.id = xtv.results_id
  where r.public_id = pid
    and xtv.path <@ (select paths from v5_dashboard.paths_by_locality pbl where pbl.locality_id = given_locality_id and pbl.results_id = results_id_from_pid);
  end
$$ language plpgsql;
