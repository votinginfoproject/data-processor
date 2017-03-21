drop materialized view if exists v5_dashboard.i18n;

create table v5_dashboard.i18n (
  results_id int references results (id) on delete cascade,
  simple_path ltree,
  value text);

with i18n as
  (select results_id, subpath(path, 0, nlevel(path) - 1) as path
   from xml_tree_values
   where simple_path ~ 'VipObject.*.Text.language'
     and value = 'en'
   order by 1)
insert into v5_dashboard.i18n
  select
    distinct on (xtv.results_id, xtv.simple_path)
    xtv.results_id, xtv.simple_path, xtv.value
  from i18n
  left join xml_tree_values xtv
         on i18n.results_id = xtv.results_id
        and xtv.simple_path ~ '*.Text'
        and  i18n.path <@ xtv.path;
