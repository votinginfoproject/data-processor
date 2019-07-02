drop function if exists v5_dashboard.locality_stats (integer, text);

create function v5_dashboard.locality_stats(rid int, lid text)
returns table(results_id int, id text, name text, type text, error_count int,
              precinct_errors int, precinct_count int, precinct_completion int,
              polling_location_errors int, polling_location_count int, polling_location_completion int,
              street_segment_errors int, street_segment_count int, street_segment_completion int,
              hours_open_errors int, hours_open_count int, hours_open_completion int,
              election_administration_errors int, election_administration_count int, election_administration_completion int,
              department_errors int, department_count int, department_completion int,
              voter_service_errors int, voter_service_count int, voter_service_completion int,
              locality_duration float, pbl_duration float, precinct_polling_location_ids_duration float,
              precinct_polling_locations_duration float, precinct_errors_duration float,
              precincts_duration float, precinct_completion_duration float, street_segments_duration float,
              polling_location_errors_duration float, polling_location_count_duration float,
              polling_location_completion_duration float, street_segment_errors_duration float,
              street_segment_completion_duration float, hours_open_errors_duration float,
              hours_open_count_duration float, hours_open_completion_duration float,
              election_administration_errors_duration float, election_administration_completion_duration float,
              department_errors_duration float, department_count_duration float,
              department_completion_duration float, voter_service_errors_duration float,
              voter_service_count_duration float, voter_service_completion_duration float,
              overall_duration float)
as $$
  declare
    locality record;
    pbl ltree[];
    precinct_polling_location_ids text[];
    precinct_polling_locations ltree[];
    locality_ea ltree[];
    precincts text[];
    street_segments ltree[];
    precinct_errors int;
    precinct_completion int;
    street_segment_errors int;
    street_segment_count int;
    street_segment_completion int;
    polling_location_errors int;
    polling_location_count int;
    polling_location_completion int;
    hours_open_errors int;
    hours_open_count int;
    hours_open_completion int;
    election_administration_errors int;
    election_administration_count int;
    election_administration_completion int;
    department_count int;
    error_count int;
    street_segment_validations text;
    start_timestamp timestamp;
    locality_duration float;
    pbl_duration float;
    precinct_polling_location_ids_duration float;
    precinct_polling_locations_duration float;
    precinct_errors_duration float;
    precincts_duration float;
    precinct_completion_duration float;
    street_segments_duration float;
    polling_location_errors_duration float;
    polling_location_count_duration float;
    polling_location_completion_duration float;
    street_segment_errors_duration float;
    street_segment_completion_duration float;
    hours_open_errors_duration float;
    hours_open_count_duration float;
    hours_open_completion_duration float;
    election_administration_errors_duration float;
    election_administration_completion_duration float;
    department_errors_duration float;
    department_count_duration float;
    department_completion_duration float;
    voter_service_errors_duration float;
    voter_service_count_duration float;
    voter_service_completion_duration float;
    overall_duration float;
  begin
    start_timestamp := clock_timestamp();
    select ct.parent_with_id, ct.name, ct.type, ct.id
    into locality
    from crosstab('select xtv.parent_with_id, subpath(xtv.simple_path, -1) as element, xtv.value
                   from xml_tree_values xtv
                   where xtv.results_id = ''' || rid || '''
                   and xtv.simple_path in (''VipObject.Locality.id'',
                   ''VipObject.Locality.Name'',
                   ''VipObject.Locality.Type'')
                   and xtv.path <@ (select subpath(xtv.parent_with_id, 0, 4)
                                    from xml_tree_values xtv
                                    where xtv.results_id = ''' || rid || '''
                                      and xtv.simple_path = ''VipObject.Locality.id''
                                      and xtv.value = ''' || lid || ''')
                                    order by parent_with_id, element',
                    'select unnest(ARRAY[''Name'', ''Type'', ''id''])' )
    as ct(parent_with_id ltree, name text, type text, id text);
    locality_duration := EXTRACT(MILLISECONDS FROM (clock_timestamp() - start_timestamp));
    start_timestamp := clock_timestamp();
    pbl := v5_dashboard.precincts_by_locality(rid, lid);
    pbl_duration := EXTRACT(MILLISECONDS FROM (clock_timestamp() - start_timestamp));

    start_timestamp := clock_timestamp();
    select string_to_array(string_agg(precincts.value, ' '), ' ')
    into precinct_polling_location_ids
    from xml_tree_values precincts
    where precincts.results_id = rid
      and precincts.simple_path = 'VipObject.Precinct.PollingLocationIds'
      and precincts.parent_with_id <@ pbl;
    precinct_polling_location_ids_duration := EXTRACT(MILLISECONDS FROM (clock_timestamp() - start_timestamp));

    start_timestamp := clock_timestamp();
    select array_agg(subpath(polling_locations.parent_with_id, 0, 4))
    into precinct_polling_locations
    from xml_tree_values polling_locations
    where polling_locations.results_id = rid
    and polling_locations.simple_path = 'VipObject.PollingLocation.id'
    and polling_locations.value in (select unnest(precinct_polling_location_ids));
    precinct_polling_locations_duration := EXTRACT(MILLISECONDS FROM (clock_timestamp() - start_timestamp));

    start_timestamp := clock_timestamp();
    select count(subpath(errors.path, 0, 4)) as precincts
    into precinct_errors
    from xml_tree_validations errors
    where errors.results_id = rid
      and errors.path ~ 'VipObject.*{1}.Precinct.*'
      and errors.path <@ pbl
      or errors.error_data = '"' || lid || '"';
    precinct_errors_duration := EXTRACT(MILLISECONDS FROM (clock_timestamp() - start_timestamp));

    start_timestamp := clock_timestamp();
    select array_agg(precincts.value)
    into precincts
    from xml_tree_values precincts
    where precincts.results_id = rid
      and precincts.simple_path = 'VipObject.Precinct.id'
      and precincts.path <@ pbl;
    precincts_duration := EXTRACT(MILLISECONDS FROM (clock_timestamp() - start_timestamp));

    start_timestamp := clock_timestamp();
    precinct_completion := v5_dashboard.completion_rate(cardinality(precincts), precinct_errors);
    precinct_completion_duration := EXTRACT(MILLISECONDS FROM (clock_timestamp() - start_timestamp));

    start_timestamp := clock_timestamp();
    select array_agg(subpath(xtv.parent_with_id, 0, 4))
    into street_segments
    from xml_tree_values xtv
    where xtv.results_id = rid
      and xtv.simple_path = 'VipObject.StreetSegment.PrecinctId'
      and xtv.value in (select unnest(precincts));
    street_segments_duration := EXTRACT(MILLISECONDS FROM (clock_timestamp() - start_timestamp));

    start_timestamp := clock_timestamp();
    select count(subpath(polling_location_errors.path, 0, 4))
    into polling_location_errors
    from xml_tree_validations polling_location_errors
    where polling_location_errors.results_id = rid
      and polling_location_errors.path ~ 'VipObject.0.PollingLocation.*'
      and polling_location_errors.path <@ precinct_polling_locations;
    polling_location_errors_duration := EXTRACT(MILLISECONDS FROM (clock_timestamp() - start_timestamp));

    start_timestamp := clock_timestamp();
    polling_location_count := cardinality(precinct_polling_locations);
    polling_location_count_duration := EXTRACT(MILLISECONDS FROM (clock_timestamp() - start_timestamp));

    start_timestamp := clock_timestamp();
    polling_location_completion := v5_dashboard.completion_rate(polling_location_count, polling_location_errors);
    polling_location_completion_duration := EXTRACT(MILLISECONDS FROM (clock_timestamp() - start_timestamp));

    street_segment_validations := 'v5_street_segment_validations_' || rid;

    start_timestamp := clock_timestamp();
    with paths as (
      select unnest (paths) as path
      from v5_dashboard.paths_by_locality pbl
      where pbl.results_id = rid
        and pbl.locality_id = lid)
    select count (v.path)
    into street_segment_errors
    from paths p, street_segment_validations v
    where v.results_id = rid
      and v.path <@ p.path;
    street_segment_errors_duration := EXTRACT(MILLISECONDS FROM (clock_timestamp() - start_timestamp));

    start_timestamp := clock_timestamp();
    street_segment_completion := v5_dashboard.completion_rate(cardinality(street_segments), street_segment_errors);
    street_segment_completion_duration := EXTRACT(MILLISECONDS FROM (clock_timestamp() - start_timestamp));

    start_timestamp := clock_timestamp();
    select count(subpath(errors.path, 0, 4))
    into hours_open_errors
    from xml_tree_validations errors
    where errors.results_id = rid
    and errors.path ~ 'VipObject.0.HoursOpen.*'
    and errors.path <@ (select array_agg(hours_open.parent_with_id)
                        from xml_tree_values hours_open
                        where hours_open.results_id = rid
                        and hours_open.simple_path = 'VipObject.HoursOpen.id'
                        and hours_open.value in -- Schedules for a Polling Location in a Precinct in a Locality
                        (select polling_location.value as hours_open_id
                         from xml_tree_values polling_location
                         where polling_location.results_id = rid
                         and polling_location.simple_path = 'VipObject.PollingLocation.HoursOpenId'
                         and polling_location.parent_with_id <@ precinct_polling_locations));
    hours_open_errors_duration := EXTRACT(MILLISECONDS FROM (clock_timestamp() - start_timestamp));

    start_timestamp := clock_timestamp();
    select count(hours_open.value) as hours_open
    into hours_open_count
    from xml_tree_values hours_open
    where hours_open.results_id = rid
      and hours_open.simple_path = 'VipObject.HoursOpen.id'
      and hours_open.value in -- Schedules for a Polling Location in a Precinct in a Locality
                           (select polling_location.value as hours_open_id
                           from xml_tree_values polling_location
                           where polling_location.results_id = rid
                             and polling_location.simple_path = 'VipObject.PollingLocation.HoursOpenId'
                             and polling_location.parent_with_id <@ precinct_polling_locations);
    hours_open_count_duration := EXTRACT(MILLISECONDS FROM (clock_timestamp() - start_timestamp));

    start_timestamp := clock_timestamp();
    hours_open_completion := v5_dashboard.completion_rate(hours_open_count, hours_open_errors);
    hours_open_completion_duration := EXTRACT(MILLISECONDS FROM (clock_timestamp() - start_timestamp));

    locality_ea := v5_dashboard.locality_election_administration(rid, lid);

    start_timestamp := clock_timestamp();
    select count(errors.path)
      into election_administration_errors
      from xml_tree_validations errors
      where errors.results_id = rid
        and element_type(errors.path) = 'ElectionAdministration'
      and errors.path <@ locality_ea;
    election_administration_errors_duration := EXTRACT(MILLISECONDS FROM (clock_timestamp() - start_timestamp));

    start_timestamp := clock_timestamp();
    election_administration_completion := v5_dashboard.completion_rate(cardinality(locality_ea), election_administration_errors);
    election_administration_completion_duration := EXTRACT(MILLISECONDS FROM (clock_timestamp() - start_timestamp));

    start_timestamp := clock_timestamp();
    select count(xtv.path)
    into department_errors
    from xml_tree_validations xtv
    where xtv.results_id = rid
      and xtv.path ~ 'VipObject.0.ElectionAdministration.*{1}.Department.*{1}'
      and xtv.path <@ locality_ea;
    department_errors_duration := EXTRACT(MILLISECONDS FROM (clock_timestamp() - start_timestamp));

    start_timestamp := clock_timestamp();
    select count(xtv.path)
    into department_count
    from xml_tree_values xtv
    where xtv.results_id = rid
      and element_type(xtv.path) = 'Department'
      and xtv.parent_with_id <@ locality_ea;
    department_count_duration := EXTRACT(MILLISECONDS FROM (clock_timestamp() - start_timestamp));

    start_timestamp := clock_timestamp();
    department_completion := v5_dashboard.completion_rate(department_count, department_errors);
    department_completion_duration := EXTRACT(MILLISECONDS FROM (clock_timestamp() - start_timestamp));

    start_timestamp := clock_timestamp();
    select count(xtv.path)
    into voter_service_errors
    from xml_tree_validations xtv
    where xtv.results_id = rid
      and element_type(xtv.path) = 'VoterService'
      and xtv.path <@ locality_ea;
    voter_service_errors_duration := EXTRACT(MILLISECONDS FROM (clock_timestamp() - start_timestamp));

    start_timestamp := clock_timestamp();
    select count(distinct subpath(xtv.path, 0, 6))
    into voter_service_count
    from xml_tree_values xtv
    where xtv.results_id = rid
      and element_type(xtv.path) = 'VoterService'
      and xtv.parent_with_id <@ locality_ea;
    voter_service_count_duration := EXTRACT(MILLISECONDS FROM (clock_timestamp() - start_timestamp));

    start_timestamp := clock_timestamp();
    voter_service_completion := v5_dashboard.completion_rate(voter_service_count, voter_service_errors);
    voter_service_completion_duration := EXTRACT(MILLISECONDS FROM (clock_timestamp() - start_timestamp));

    error_count := precinct_errors + polling_location_errors + street_segment_errors +
                   hours_open_errors + election_administration_errors + department_errors +
                   voter_service_errors;

    overall_duration := locality_duration + pbl_duration + precinct_polling_location_ids_duration +
      precinct_polling_locations_duration + precinct_errors_duration + precincts_duration +
      precinct_completion_duration + street_segments_duration + polling_location_errors_duration +
      polling_location_count_duration + polling_location_completion_duration + street_segment_errors_duration +
      street_segment_completion_duration + hours_open_errors_duration + hours_open_count_duration +
      hours_open_completion_duration + election_administration_errors_duration +
      election_administration_completion_duration + department_errors_duration +
      department_count_duration + department_completion_duration + voter_service_errors_duration +
      voter_service_count_duration + voter_service_completion_duration;

    return query
    select
      rid, locality.id, locality.name, locality.type, error_count,
      precinct_errors, cardinality(precincts), precinct_completion,
      polling_location_errors, polling_location_count, polling_location_completion,
      street_segment_errors, cardinality(street_segments), street_segment_completion,
      hours_open_errors, hours_open_count, hours_open_completion,
      election_administration_errors, cardinality(locality_ea), election_administration_completion,
      department_errors, department_count, department_completion,
      voter_service_errors, voter_service_count, voter_service_completion,
      locality_duration, pbl_duration, precinct_polling_location_ids_duration,
      precinct_polling_locations_duration, precinct_errors_duration,
      precincts_duration, precinct_completion_duration, street_segments_duration,
      polling_location_errors_duration, polling_location_count_duration,
      polling_location_completion_duration,street_segment_errors_duration,
      street_segment_completion_duration, hours_open_errors_duration,
      hours_open_count_duration, hours_open_completion_duration,
      election_administration_errors_duration,
      election_administration_completion_duration,
      department_errors_duration, department_count_duration,
      department_completion_duration, voter_service_errors_duration,
      voter_service_count_duration, voter_service_completion_duration, overall_duration;
  end;
$$ language plpgsql;
