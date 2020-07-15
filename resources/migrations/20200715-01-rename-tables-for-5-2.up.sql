ALTER TABLE IF EXISTS v5_1_ballot_measure_contests RENAME TO v5_2_ballot_measure_contests;
ALTER TABLE IF EXISTS v5_1_ballot_measure_selections RENAME TO v5_2_ballot_measure_selections;
ALTER TABLE IF EXISTS v5_1_ballot_selections RENAME TO v5_2_ballot_selections;
ALTER TABLE IF EXISTS v5_1_ballot_styles RENAME TO v5_2_ballot_styles;
ALTER TABLE IF EXISTS v5_1_candidate_contests RENAME TO v5_2_candidate_contests;
ALTER TABLE IF EXISTS v5_1_candidate_selections RENAME TO v5_2_candidate_selections;
ALTER TABLE IF EXISTS v5_1_candidates RENAME TO v5_2_candidates;
ALTER TABLE IF EXISTS v5_1_contact_information RENAME TO v5_2_contact_information;
ALTER TABLE IF EXISTS v5_1_contests RENAME TO v5_2_contests;
ALTER TABLE IF EXISTS v5_1_departments RENAME TO v5_2_departments;
ALTER TABLE IF EXISTS v5_1_election_administrations RENAME TO v5_2_election_administrations;
ALTER TABLE IF EXISTS v5_1_elections RENAME TO v5_2_elections;
ALTER TABLE IF EXISTS v5_1_electoral_districts RENAME TO v5_2_electoral_districts;
ALTER TABLE IF EXISTS v5_1_localities RENAME TO v5_2_localities;
ALTER TABLE IF EXISTS v5_1_offices RENAME TO v5_2_offices;
ALTER TABLE IF EXISTS v5_1_ordered_contests RENAME TO v5_2_ordered_contests;
ALTER TABLE IF EXISTS v5_1_parties RENAME TO v5_2_parties;
ALTER TABLE IF EXISTS v5_1_party_contests RENAME TO v5_2_party_contests;
ALTER TABLE IF EXISTS v5_1_party_selections RENAME TO v5_2_party_selections;
ALTER TABLE IF EXISTS v5_1_people RENAME TO v5_2_people;
ALTER TABLE IF EXISTS v5_1_polling_locations RENAME TO v5_2_polling_locations;
ALTER TABLE IF EXISTS v5_1_precincts RENAME TO v5_2_precincts;
ALTER TABLE IF EXISTS v5_1_retention_contests RENAME TO v5_2_retention_contests;
ALTER TABLE IF EXISTS v5_1_schedules RENAME TO v5_2_schedules;
ALTER TABLE IF EXISTS v5_1_sources RENAME TO v5_2_sources;
ALTER TABLE IF EXISTS v5_1_states RENAME TO v5_2_states;
ALTER TABLE IF EXISTS v5_1_street_segments RENAME TO v5_2_street_segments;
ALTER TABLE IF EXISTS v5_1_voter_services RENAME TO v5_2_voter_services;

create or replace function street_segment_overlaps(rid int)
returns table (path ltree, id text)
as $$
declare
  ss_temp text;
  ss_temp_idx text;
begin
  ss_temp := 'street_segments_' || rid;
  ss_temp_idx := ss_temp || '_idx';

  execute 'create temp table ' || ss_temp || ' on commit drop as
     select
       ss.id,
       ss.results_id,
       ss.street_name,
       ss.state,
       ss.zip,
       ss.odd_even_both,
       ss.precinct_id,
       ss.start_house_number,
       ss.end_house_number,
       coalesce(ss.address_direction, ''NULL_VALUE'') as address_direction,
       coalesce(ss.street_direction, ''NULL_VALUE'') as street_direction,
       coalesce(ss.street_suffix, ''NULL_VALUE'') as street_suffix
     from v5_2_street_segments ss
     where ss.results_id = ' || rid ||
     'order by ss.street_name, ss.state, ss.zip'
   using rid;

  execute 'create index ' || ss_temp_idx ||
          ' on ' || ss_temp || ' (street_name, state, zip)';

  execute 'analyze ' || ss_temp;

  return query
  execute 'SELECT subpath(xtv.path,0,4), ss2.id
   FROM ' || ss_temp::regclass || ' ss
   INNER JOIN ' || ss_temp::regclass || ' ss2
           ON ss2.start_house_number >= ss.start_house_number AND
              ss2.start_house_number <= ss.end_house_number AND
              ss.street_name = ss2.street_name AND
              ss.state = ss2.state AND
              ss.zip = ss2.zip AND
              ss.address_direction = ss2.address_direction AND
              ss.street_direction = ss2.street_direction AND
              ss.street_suffix = ss2.street_suffix AND
              ss.precinct_id != ss2.precinct_id AND
              ss.id != ss2.id AND
              (ss.odd_even_both = ss2.odd_even_both OR
               ss.odd_even_both = ''both'' OR
               ss2.odd_even_both = ''both'')
   INNER JOIN xml_tree_values xtv
           ON xtv.value = ss.id AND xtv.results_id = ' || rid ||
   'WHERE xtv.simple_path = ''VipObject.StreetSegment.id''';
end;
$$ language plpgsql;
