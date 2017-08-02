create or replace function
v5_dashboard.feed_localities(rid int)
returns void as $$
declare
  locality record;
  street_segment_validations text;
  street_segment_validations_idx text;
begin

  street_segment_validations := 'v5_street_segment_validations_' || rid;
  street_segment_validations_idx := 'v5_street_segment_paths_' || rid;

  create temp table street_segment_validations on commit drop as
    select * from xml_tree_validations
    where results_id = rid
      and path ~ 'VipObject.0.StreetSegment.*';

  create index street_segment_validations_idx on street_segment_validations using gist (path);

  analyze street_segment_validations;

  for locality in
    select xtv.value as id
    from xml_tree_values xtv
    where xtv.simple_path = 'VipObject.Locality.id'
      and xtv.results_id = rid
  loop
    execute 'insert into v5_dashboard.localities
             select *
             from v5_dashboard.locality_stats('|| rid || ', ''' || locality.id || ''' :: text)';
  end loop;

  drop table street_segment_validations;

end;
$$ language plpgsql;
