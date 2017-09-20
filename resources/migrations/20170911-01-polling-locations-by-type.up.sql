drop function if exists v5_dashboard.polling_locations_by_type (integer);

create function v5_dashboard.polling_locations_by_type(rid int)
returns void
as $$
  declare
    polling_location_all_paths ltree[];
    polling_location_paths ltree[];
    polling_location_count int;
    pl_errors int;
    pl_completion int;
    db_polling_location_paths ltree[];
    db_polling_location_count int;
    db_pl_errors int;
    db_pl_completion int;
    ev_polling_location_paths ltree[];
    ev_polling_location_count int;
    ev_pl_errors int;
    ev_pl_completion int;
  begin

  /*get all polling location paths*/
  select array_agg(distinct countable_path(path)) into polling_location_all_paths
    from xml_tree_values values
      where values.results_id = rid
        and values.path ~ 'VipObject.0.PollingLocation.*'
        and values.path is not null;

  /*get all top-level polling locations with the attribute IsDropBox set to true*/
  select array_agg(parent_with_id) into db_polling_location_paths
    from xml_tree_values values
    where values.results_id = rid
      and values.path ~ 'VipObject.0.PollingLocation.*.IsDropBox.*'
      and values.path IS NOT NULL
      and value = 'true';

  /*get a count of errors that are descendents of drop box polling locations*/
  select count(*) into db_pl_errors
  from xml_tree_validations xtv
  where xtv.path <@ db_polling_location_paths
  and xtv.results_id = rid;

  /*get all top-level polling locations with the attribute IsEarlyVoting set to true*/
  select array_agg(parent_with_id) into ev_polling_location_paths
    from xml_tree_values values
    where values.results_id = rid
      and values.path ~ 'VipObject.0.PollingLocation.*.IsEarlyVoting.*'
      and values.path IS NOT NULL
      and value = 'true';

  /*get a count of errors that are descendents of early vote site polling locations*/
  select count(*) into ev_pl_errors
  from xml_tree_validations xtv
  where xtv.path <@ ev_polling_location_paths
  and xtv.results_id = rid;

  /*the regular polling locations will be all the polling locations that are _not_ in
    the list of early vote site or drop box polling locations*/
  with polling_location_paths as(
    select distinct countable_path(path) pl_path
      from xml_tree_values values
        where values.results_id = rid
          and values.path ~ 'VipObject.0.PollingLocation.*'
          and values.path is not null
  ),
  db_polling_location_paths as (
  select parent_with_id pl_path
    from xml_tree_values values
    where values.results_id = rid
      and values.path ~ 'VipObject.0.PollingLocation.*.IsDropBox.*'
      and values.path IS NOT NULL
      and value = 'true'
  ),
  ev_polling_location_paths as (
  select parent_with_id pl_path
    from xml_tree_values values
    where values.results_id = rid
      and values.path ~ 'VipObject.0.PollingLocation.*.IsEarlyVoting.*'
      and values.path IS NOT NULL
      and value = 'true'
  ),
  regular_polling_locations as (
  select pl_path from polling_location_paths except (select pl_path from db_polling_location_paths union select pl_path from ev_polling_location_paths))
  select array_agg(pl_path) from regular_polling_locations into polling_location_paths;

  /*get a count of all the polling location errors that are descendents of the regular polling locations*/
  select count(*) into pl_errors
  from xml_tree_validations xtv
  where xtv.path <@ polling_location_paths
  and xtv.results_id = rid;

  /*calculate the completion rate for each of the polling location types*/
  select
    case
      when cardinality(polling_location_paths) < pl_errors
        then 0
      when pl_errors = 0
        then 100
      else floor((cardinality(polling_location_paths) - pl_errors)/cardinality(polling_location_paths))
    into pl_completion
  end;

  select
    case
      when cardinality(db_polling_location_paths) < db_pl_errors
        then 0
      when db_pl_errors = 0
        then 100
      else floor((cardinality(db_polling_location_paths) - db_pl_errors)/cardinality(db_polling_location_paths))
    into db_pl_completion
  end;

  select
    case
      when cardinality(ev_polling_location_paths) < ev_pl_errors
        then 0
      when ev_pl_errors = 0
        then 100
      else floor((cardinality(ev_polling_location_paths) - ev_pl_errors)/cardinality(ev_polling_location_paths))
    into ev_pl_completion
  end;

  /*use all the data collected to update the v5_statistics table*/
  update v5_statistics v
    set polling_location_count = cardinality(polling_location_paths),
        polling_location_errors = pl_errors,
        polling_location_completion = pl_completion,
        db_polling_location_count = cardinality(db_polling_location_paths),
        db_polling_location_errors = db_pl_errors,
        db_polling_location_completion = db_pl_errors,
        ev_polling_location_count = cardinality(ev_polling_location_paths),
        ev_polling_location_errors = ev_pl_errors,
        ev_polling_location_completion = ev_pl_completion
    where v.results_id = rid;

  end;
$$ language plpgsql;
