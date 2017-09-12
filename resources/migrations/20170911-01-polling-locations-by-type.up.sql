drop function if exists v5_dashboard.polling_locations_by_type (integer);

create function v5_dashboard.polling_locations_by_type(rid int)
returns table(results_id int,
              polling_location_count int,
              pl_errors int,
              pl_completion int,
              db_polling_location_count int,
              db_pl_errors int,
              db_pl_completion int,
              ev_polling_location_count int,
              ev_pl_errors int,
              ev_pl_completion int)
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

  select array_agg(distinct countable_path(path)) into polling_location_all_paths
    FROM xml_tree_values values
      WHERE values.results_id = rid
        AND values.path ~ 'VipObject.0.PollingLocation.*'
        and values.path is not null;

  SELECT array_agg(parent_with_id) into db_polling_location_paths
    FROM xml_tree_values values
    WHERE values.results_id = rid
      AND values.path ~ 'VipObject.0.PollingLocation.*.IsDropBox.*'
      AND values.path IS NOT NULL
      AND value = 'true';

  select count(*) into db_pl_errors
  from xml_tree_validations xtv
  where xtv.path <@ db_polling_location_paths
  and xtv.results_id = rid;

  SELECT array_agg(parent_with_id) into ev_polling_location_paths
    FROM xml_tree_values values
    WHERE values.results_id = rid
      AND values.path ~ 'VipObject.0.PollingLocation.*.IsEarlyVoting.*'
      AND values.path IS NOT NULL
      AND value = 'true';

  select count(*) into ev_pl_errors
  from xml_tree_validations xtv
  where xtv.path <@ ev_polling_location_paths
  and xtv.results_id = rid;

  with polling_location_paths as(
    select distinct countable_path(path) pl_path
      FROM xml_tree_values values
        WHERE values.results_id = rid
          AND values.path ~ 'VipObject.0.PollingLocation.*'
          and values.path is not null
  ),
  db_polling_location_paths as (
  SELECT parent_with_id pl_path
    FROM xml_tree_values values
    WHERE values.results_id = rid
      AND values.path ~ 'VipObject.0.PollingLocation.*.IsDropBox.*'
      AND values.path IS NOT NULL
      AND value = 'true'
  ),
  ev_polling_location_paths as (
  SELECT parent_with_id pl_path
    FROM xml_tree_values values
    WHERE values.results_id = rid
      AND values.path ~ 'VipObject.0.PollingLocation.*.IsEarlyVoting.*'
      AND values.path IS NOT NULL
      AND value = 'true'
  ),
  regular_polling_locations as (
  select pl_path from polling_location_paths except (select pl_path from db_polling_location_paths union select pl_path from ev_polling_location_paths))
  select array_agg(pl_path) from regular_polling_locations into polling_location_paths;

  select count(*) into pl_errors
  from xml_tree_validations xtv
  where xtv.path <@ polling_location_paths
  and xtv.results_id = rid;

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


    return query
    select
      rid, cardinality(polling_location_paths), pl_errors, pl_completion,
      cardinality(db_polling_location_paths), db_pl_errors, db_pl_completion,
      cardinality(ev_polling_location_paths), ev_pl_errors, ev_pl_completion;

  end;
$$ language plpgsql;
