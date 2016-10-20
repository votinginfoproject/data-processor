create or replace function locality_stats(rid text, locality_id text)
returns table(id text, name text, type text, error_count int,
              precinct_errors int, precinct_count int, precinct_completion int,
              polling_location_errors int, polling_location_count int, polling_location_completion int,
              street_segment_errors int, street_segment_count int, street_segment_completion int,
              hours_open_errors int, hours_open_count int, hours_open_completion int,
              election_administration_errors int, election_administration_count int, election_administration_completion int,
              department_errors int, department_count int, department_completion int,
              voter_service_errors int, voter_service_count int, voter_service_completion int)
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
  begin
    select ct.parent_with_id, ct.name, ct.type, ct.id
    into locality
    from crosstab('select xtv.parent_with_id, subpath(xtv.simple_path, -1) as element, xtv.value
                   from results r
                   left join xml_tree_values xtv on r.id = xtv.results_id
                   where r.public_id = ''' || rid || '''
                   and xtv.simple_path in (''VipObject.Locality.id'',
                   ''VipObject.Locality.Name'',
                   ''VipObject.Locality.Type'')
                   and xtv.path <@ (select subpath(xtv.parent_with_id, 0, 4) from results r
                                    left join xml_tree_values xtv on r.id = xtv.results_id
                                    where r.public_id = ''' || rid || '''
                                      and xtv.simple_path = ''VipObject.Locality.id''
                                      and xtv.value = ''' || locality_id || ''')
                                    order by parent_with_id, element')
    as ct(parent_with_id ltree, name text, type text, id text);
  pbl := precincts_by_locality(locality_id, rid);

  select array_agg(string_to_array(precincts.value, ' '))
  into precinct_polling_location_ids
  from results r
  left join xml_tree_values precincts on precincts.results_id = r.id
  where r.public_id = rid
    and precincts.simple_path = 'VipObject.Precinct.PollingLocationIds'
    and precincts.parent_with_id <@ pbl;

  select array_agg(subpath(polling_locations.parent_with_id, 0, 4))
  into precinct_polling_locations
  from results r
  left join xml_tree_values polling_locations on polling_locations.results_id = r.id
  where r.public_id = rid
  and polling_locations.simple_path = 'VipObject.PollingLocation.id'
  and polling_locations.value in (select unnest(precinct_polling_location_ids));

  select count(subpath(errors.path, 0, 4)) as precincts
  into precinct_errors
  from results r
  left join xml_tree_validations errors on r.id = errors.results_id
  where r.public_id = rid
    and errors.path ~ '*.Precinct.*'
    and errors.path <@ pbl
    or errors.error_data = '"' || locality_id || '"';

  select array_agg(precincts.value)
  into precincts
  from results r
  left join xml_tree_values precincts on precincts.results_id = r.id
  where r.public_id = rid
    and precincts.simple_path = 'VipObject.Precinct.id'
    and precincts.path <@ pbl;

  precinct_completion := completion_rate(cardinality(precincts), precinct_errors);

  select array_agg(subpath(xtv.parent_with_id, 0, 4))
  into street_segments
  from results r
  left join xml_tree_values xtv
  on xtv.results_id = r.id
  where r.public_id = rid
    and xtv.simple_path = 'VipObject.StreetSegment.PrecinctId'
    and xtv.value in (select unnest(precincts));

  select count(subpath(polling_location_errors.path, 0, 4))
  into polling_location_errors
  from results r
  left join xml_tree_validations polling_location_errors on r.id = polling_location_errors.results_id
  where r.public_id = rid
    and polling_location_errors.path ~ '*.PollingLocation.*'
    and polling_location_errors.path <@ precinct_polling_locations;

  polling_location_count := cardinality(precinct_polling_locations);
  polling_location_completion := completion_rate(polling_location_count, polling_location_errors);

  select count(subpath(errors.path, 0, 4))
  into street_segment_errors
  from results r
  left join xml_tree_validations errors on r.id = errors.results_id
  where r.public_id = rid
    and errors.path <@ street_segments
    and errors.path ~ '*.StreetSegment.*';


  street_segment_completion := completion_rate(cardinality(street_segments), street_segment_errors);

  -- TODO: extract the 'schedules for a polling location in a precinct in a locality' part.
  select count(subpath(errors.path, 0, 4))
  into hours_open_errors
  from results r
  left join xml_tree_validations errors on r.id = errors.results_id
  where r.public_id = rid
  and errors.path ~ '*.HoursOpen.*'
  and errors.path <@ (select array_agg(hours_open.parent_with_id)
                      from results r
                      left join xml_tree_values hours_open on hours_open.results_id = r.id
                      where r.public_id = rid
                      and hours_open.simple_path = 'VipObject.HoursOpen.id'
                      and hours_open.value in -- Schedules for a Polling Location in a Precinct in a Locality
                      (select polling_location.value as hours_open_id
                       from results r
                       left join xml_tree_values polling_location on polling_location.results_id = r.id
                       where r.public_id = rid
                       and polling_location.simple_path = 'VipObject.PollingLocation.HoursOpenId'
                       and polling_location.parent_with_id <@ precinct_polling_locations));

  select count(hours_open.value) as hours_open
  into hours_open_count
  from results r
  left join xml_tree_values hours_open on hours_open.results_id = r.id
  where r.public_id = rid
  and hours_open.simple_path = 'VipObject.HoursOpen.id'
  and hours_open.value in -- Schedules for a Polling Location in a Precinct in a Locality
                       (select polling_location.value as hours_open_id
                       from results r
                       left join xml_tree_values polling_location on polling_location.results_id = r.id
                       where r.public_id = rid
                       and polling_location.simple_path = 'VipObject.PollingLocation.HoursOpenId'
                       and polling_location.parent_with_id <@ precinct_polling_locations);
  hours_open_completion := completion_rate(hours_open_count, hours_open_errors);

  locality_ea := locality_election_administration(locality_id, rid);

select count(errors.path)
  into election_administration_errors
  from results r
  left join xml_tree_validations errors on errors.results_id = r.id
  where r.public_id = rid
  and element_type(errors.path) = 'ElectionAdministration'
  and errors.path <@ locality_ea;

  election_administration_completion := completion_rate(cardinality(locality_ea), election_administration_errors);

  select count(xtv.path)
  into department_errors
  from results r
  left join xml_tree_validations xtv on xtv.results_id = r.id
  where r.public_id = rid
  and xtv.path ~ '*.Department.*{1}'
  and xtv.path <@ locality_ea;

  select count(xtv.path)
  into department_count
  from results r
  left join xml_tree_values xtv on xtv.results_id = r.id
  where r.public_id = rid
  and element_type(xtv.path) = 'Department'
  and xtv.parent_with_id <@ locality_ea;

  department_completion := completion_rate(department_count, department_errors);

  select count(xtv.path)
  into voter_service_errors
  from results r
  left join xml_tree_validations xtv on xtv.results_id = r.id
  where r.public_id = rid
  and element_type(xtv.path) = 'VoterService'
  and xtv.path <@ locality_ea;

  select count(distinct subpath(xtv.path, 0, 6))
  into voter_service_count
  from results r
  left join xml_tree_values xtv on xtv.results_id = r.id
  where r.public_id = rid
  and element_type(xtv.path) = 'VoterService'
  and xtv.parent_with_id <@ locality_ea;

  voter_service_completion := completion_rate(voter_service_count, voter_service_errors);

  error_count := precinct_errors + polling_location_errors + street_segment_errors +
                 hours_open_errors + election_administration_errors + department_errors +
                 voter_service_errors;
  return query
  select
    locality.id, locality.name, locality.type, error_count,
    precinct_errors, cardinality(precincts), precinct_completion,
    polling_location_errors, polling_location_count, polling_location_completion,
    street_segment_errors, cardinality(street_segments), street_segment_completion,
    hours_open_errors, hours_open_count, hours_open_completion,
    election_administration_errors, cardinality(locality_ea), election_administration_completion,
    department_errors, department_count, department_completion,
    voter_service_errors, voter_service_count, voter_service_completion;
  end;
$$ language plpgsql;
