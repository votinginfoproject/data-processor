create or replace function v5_dashboard.locality_error_report(pid text, given_locality_id text)
returns table (results_id bigint, path text, severity varchar, scope varchar, identifier text, error_type varchar, error_data text)
as $$
declare results_id_from_pid int;
begin
  select id into results_id_from_pid from results where results.public_id = pid;
  return query
  with locality_paths as (
    select unnest(paths) as path
    from v5_dashboard.paths_by_locality pbl
    where pbl.results_id = results_id_from_pid
      and pbl.locality_id = given_locality_id)
  select xtv.results_id, ltree2text(xtv.path), xtv.severity, xtv.scope, xtv.parent_element_id as identifier, xtv.error_type, xtv.error_data
  from xml_tree_validations xtv, locality_paths p
  where xtv.results_id = results_id_from_pid
    and xtv.path <@ p.path;
  end
$$ language plpgsql;
