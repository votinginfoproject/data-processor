create or replace function v5_dashboard.populate_locality_table(rid integer)
returns void as $$
declare
  localities record;
  precincts ltree[];
  precincts_values text[];
  polling_locations ltree[];
  hours_open ltree[];
  street_segments ltree[];
  electoral_districts ltree[];
  contests ltree[];
  locality_paths ltree[];
  election_administrations ltree[];
  temp_table text;
begin
  delete from v5_dashboard.paths_by_locality where results_id = rid;
  temp_table := 'locality_stuff_' || rid;
  raise notice '% building temp table...', clock_timestamp();
  execute 'create temp table ' || temp_table || ' on commit drop as
     select path, simple_path, value, parent_with_id
     from xml_tree_values
     where results_id = ' || rid || '
     and path ~ ''VipObject.0.HoursOpen|Locality|ElectionAdministration|ElectoralDistrict|Precinct|PollingLocation|StreetSegment|Contest|BallotMeasureContest|CandidateContest|OrderedContest|RetentionContest|PartyContest.*'''
  using rid;

  raise notice '% building index 1/2...', clock_timestamp();
  execute 'create index '|| temp_table::regclass || '_ss_idx
           on ' || temp_table::regclass ||
          ' (simple_path, value)
          where simple_path = ''VipObject.StreetSegment.PrecinctId''';
  raise notice '% building index 2/2...', clock_timestamp();
  execute 'create index ' || temp_table::regclass ||'_other_idx
           on ' || temp_table::regclass ||
          ' (simple_path, path, value, parent_with_id)
          where simple_path = ''VipObject.Precinct.id''
             or simple_path = ''VipObject.Precinct.PollingLocationIds''
             or simple_path = ''VipObject.Precinct.ElectoralDistrictIds''
             or simple_path = ''VipObject.ElectoralDistrict.id''
             or simple_path = ''VipObject.Precinct.LocalityId''
             or simple_path = ''VipObject.PollingLocation.id''
             or simple_path = ''VipObject.PollingLocation.HoursOpenId''
             or simple_path = ''VipObject.Locality.id''
             or simple_path = ''VipObject.Locality.ElectionAdministrationId''
             or simple_path = ''VipObject.ElectionAdministration.id''
             or simple_path ~ ''VipObject.Contest|BallotMeasureContest|CandidateContest|OrderedContest|RetentionContest|PartyContest.*''
             or simple_path = ''VipObject.HoursOpen.id''';

  execute 'analyze ' || temp_table;
  raise notice '% about to loop...', clock_timestamp();
  for localities in
    execute 'select value as id, array_agg(parent_with_id) as precincts
             from ' || temp_table::regclass || '
             where simple_path = ''VipObject.Precinct.LocalityId''
               and parent_with_id is not null
               group by value
               order by value'
  loop
    raise notice '% beginning loop, doing street segments...', clock_timestamp();
    execute 'with precincts as (
               select array_agg(value) as v
               from ' || temp_table::regclass || '
               where simple_path = ''VipObject.Precinct.id''
                 and path <@ $1)
             select array_agg(subpath(parent_with_id, 0, 4))
             from ' || temp_table::regclass || '
             where simple_path = ''VipObject.StreetSegment.PrecinctId''
               and value in (select unnest(precincts.v) from precincts)'
    into street_segments
    using localities.precincts;
    raise notice '% doing polling locations...', clock_timestamp();
    execute 'with polling_locations as (
               select string_to_array(string_agg(value, '' ''), '' '') as ids
               from ' || temp_table::regclass || '
               where simple_path = ''VipObject.Precinct.PollingLocationIds''
                 and parent_with_id <@ $1)
             select array_agg(subpath(parent_with_id, 0, 4))
             from ' || temp_table::regclass || '
             where simple_path = ''VipObject.PollingLocation.id''
               and value in (select unnest(ids) from polling_locations)'
    into polling_locations
    using localities.precincts;
    raise notice '% doing electoral districts...', clock_timestamp();
    execute 'with districts
             as (select string_to_array(string_agg(value, '' ''), '' '') as ids
                 from ' || temp_table::regclass || '
                 where simple_path = ''VipObject.Precinct.ElectoralDistrictIds''
                   and path <@ $1)
             select array_agg(parent_with_id)
             from ' || temp_table::regclass || '
             where simple_path = ''VipObject.ElectoralDistrict.id''
               and value in (select unnest(districts.ids) from districts)'
    into electoral_districts
    using localities.precincts;
    raise notice '% doing contests...', clock_timestamp();
    execute 'with districts as (
               select string_to_array(string_agg(value, '' ''), '' '') as ids
               from ' || temp_table::regclass || '
               where simple_path = ''VipObject.Precinct.ElectoralDistrictIds''
                 and path <@ $1)
             select array_agg(parent_with_id)
             from ' || temp_table::regclass || '
             where simple_path ~ ''VipObject.Contest|BallotMeasureContest|CandidateContest|OrderedContest|RetentionContest|PartyContest.*''
               and value in (select unnest(districts.ids) from districts)'
    into contests
    using localities.precincts;
    raise notice '% doing hours open...', clock_timestamp();
    execute 'select array_agg(path)
             from ' || temp_table::regclass || '
             where simple_path = ''VipObject.HoursOpen.id''
               and value in (select value
                             from ' || temp_table::regclass || '
                             where simple_path = ''VipObject.PollingLocation.HoursOpenId''
                               and parent_with_id <@ $1)'
    into hours_open
    using polling_locations;

    raise notice '% doing election administration...', clock_timestamp();
    execute
      'with locality as
        (select * from ' || temp_table::regclass || '
            where simple_path = ''VipObject.Locality.id''
            and value = $1),
      election_admin as
        (select t.* from ' || temp_table::regclass || ' t
            join locality on locality.parent_with_id = t.parent_with_id
            where t.simple_path = ''VipObject.Locality.ElectionAdministrationId''),
      path as (
          select t.parent_with_id from ' || temp_table::regclass || ' t
          join election_admin on election_admin.value = t.value
          and t.simple_path = ''VipObject.ElectionAdministration.id'')
      select array_agg(t.path) from '|| temp_table::regclass ||' t join path on t.parent_with_id = path.parent_with_id'
    into election_administrations
    using localities.id;

    raise notice '% finishing up...', clock_timestamp();

    locality_paths :=  localities.precincts || polling_locations || hours_open || street_segments || electoral_districts || contests || election_administrations;
    insert into v5_dashboard.paths_by_locality values(rid, localities.id, locality_paths);
  end loop;
end;
$$ language plpgsql;
