create or replace function
v5_dashboard.feed_localities(rid int)
returns void as $$
declare
  locality record;
begin

  create temp table v5_street_segment_validations on commit drop as
    select * from xml_tree_validations
    where results_id = rid
      and path ~ 'VipObject.0.StreetSegment.*';

  create index v5_street_segment_paths on v5_street_segment_validations using gist (path);

  analyze v5_street_segment_validations;

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

  drop table v5_street_segment_validations;

end;
$$ language plpgsql;
