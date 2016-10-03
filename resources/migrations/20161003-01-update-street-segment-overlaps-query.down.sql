create or replace function street_segment_overlaps(rid int)
returns table (path ltree, id text)
as $$
begin
  create temp table sse on commit drop as
  select ss.id,
    ss.results_id,
    ss.street_name,
    ss.city,
    ss.state,
    ss.zip,
    ss.odd_even_both,
    ss.precinct_id,
    ss.start_house_number,
    ss.end_house_number,
    coalesce(ss.address_direction, 'NULL_VALUE') as address_direction,
    coalesce(ss.street_direction, 'NULL_VALUE') as street_direction,
    coalesce(ss.street_suffix, 'NULL_VALUE') as street_suffix
    from v5_1_street_segments ss
  where ss.results_id = rid
  order by ss.street_name, ss.city, ss.state, ss.zip;

  create index street_segment_experiment_idx on sse (street_name, city, state, zip);

  analyze sse;

  return query SELECT subpath(xtv.path,0,4), ss2.id
   FROM sse ss
   INNER JOIN sse ss2
           ON ss2.start_house_number >= ss.start_house_number AND
              ss2.start_house_number <= ss.end_house_number AND
              ss.street_name = ss2.street_name AND
              ss.city = ss2.city AND
              ss.state = ss2.state AND
              ss.zip = ss2.zip AND
              ss.address_direction = ss2.address_direction AND
              ss.street_direction = ss2.street_direction AND
              ss.street_suffix = ss2.street_suffix AND
              ss.precinct_id != ss2.precinct_id AND
              ss.id != ss2.id AND
              (ss.odd_even_both = ss2.odd_even_both OR
               ss.odd_even_both = 'both' OR
               ss2.odd_even_both = 'both')
   INNER JOIN xml_tree_values xtv
           ON xtv.value = ss.id AND xtv.results_id = rid
   WHERE xtv.simple_path = 'VipObject.StreetSegment.id';
end;
$$ language plpgsql;
