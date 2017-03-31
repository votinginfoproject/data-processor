create or replace function v3_dashboard.polling_location_readable_report(pid text)
returns table(locality_name text, precinct_name text, address_location_name text, address_line1 text, address_line2 text, address_line3 text, address_city text, address_state text, address_zip text, polling_location_id bigint)
as $$
begin
return query
select distinct on (v.identifier)
       l1.name, p1.name, pl.address_location_name, pl.address_line1,
       pl.address_line2, pl.address_line3, pl.address_city,
       pl.address_state, pl.address_zip, v.identifier
    from results r
    join validations v on r.id = v.results_id
    join v3_0_polling_locations pl
      on v.identifier = pl.id
      and pl.results_id = r.id
    left join v3_0_precinct_polling_locations ppl
      on v.identifier = ppl.polling_location_id
      and v.results_id = ppl.results_id
    left join v3_0_precinct_split_polling_locations pspl
      on v.identifier = pspl.polling_location_id
      and v.results_id = pspl.results_id
    left join v3_0_precincts p
      on ppl.precinct_id = p.id
      and p.results_id = r.id
    left join v3_0_precinct_splits ps
      on pspl.precinct_split_id = ps.id
      and ps.results_id = r.id
    left join v3_0_precincts p1
      on ps.precinct_id = p1.id
      and p1.results_id = r.id
    left join v3_0_localities l
      on l.id = p.locality_id
      and l.results_id = r.id
    left join v3_0_localities l1
      on l1.id = p1.locality_id
      and l1.results_id = r.id
    where r.public_id = pid
    and v.scope = 'polling-locations'
    and v.error_type like 'address%';
end
$$ language plpgsql;
