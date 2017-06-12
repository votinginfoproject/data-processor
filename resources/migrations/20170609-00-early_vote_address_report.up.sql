create or replace function v3_dashboard.early_vote_location_address_report(pid text)
returns table(locality_name text, early_vote_site_name text, address_location_name text, address_line1 text, address_line2 text, address_line3 text, address_city text, address_state text, address_zip text, polling_location_id bigint)
as $$
begin
return query
select distinct on (v.identifier)
       l.name, ev.name, ev.address_location_name, ev.address_line1,
       ev.address_line2, ev.address_line3, ev.address_city,
       ev.address_state, ev.address_zip, v.identifier
    from results r
    join validations v on r.id = v.results_id
    join v3_0_early_vote_sites ev
      on v.identifier = ev.id
      and ev.results_id = r.id
    join v3_0_locality_early_vote_sites lev
      on ev.id = lev.early_vote_site_id
    join v3_0_localities l
      on l.id = lev.locality_id
    where r.public_id = pid
    and v.scope = 'early-vote-sites'
    and v.error_type like 'address%';
end
$$ language plpgsql;
