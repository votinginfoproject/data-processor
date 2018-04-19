create schema if not exists cleanup;

create or replace view cleanup.feeds_by_election(state, vip_id, current_run, old_runs) as
  with feeds as
      (select r.state as state,
              r.vip_id as vip_id,
              r.spec_version as spec_version,
              r.election_type as election_type,
              r.election_date as election_date,
              array_agg(r.id order by r.id desc) as ids
       from results r
       where r.complete is true
         and r.vip_id is not null
       group by r.vip_id, r.state, r.spec_version, r.election_type, r.election_date)

  select
    state,
    vip_id,
    ids[1:5] as current_runs,
    ids[6:array_upper(ids, 1)] as old_runs
  from feeds
  where cardinality(ids) > 5;
