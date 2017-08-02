create or replace function
v5_dashboard.feed_localities(rid int)
returns void as $$
declare
  locality record;
begin
  for locality in
    select xtv.value as id
    from xml_tree_values xtv
    where xtv.simple_path = 'VipObject.Locality.id'
      and xtv.results_id = rid
  loop
    execute 'insert into v5_dashboard.localities
             select *
             from v5_dashboard.locality_stats('|| rid || ', ''' || locality.id || ''')';
  end loop;
end;
$$ language plpgsql;
