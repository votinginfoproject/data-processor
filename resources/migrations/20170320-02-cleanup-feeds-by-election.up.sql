create schema if not exists cleanup;

create or replace view cleanup.feeds_by_election(state, vip_id, current_run, old_runs) as
  with feeds as
      (select xtv_state.value as state,
                        xtv_source.value as vip_id,
                        r.election_type as election_type,
                        r.election_date as election_date,
                        array_agg(r.id order by r.id desc) as ids
                 from results r
                 left join xml_tree_values xtv_state
                   on xtv_state.results_id = r.id and xtv_state.simple_path = 'VipObject.State.Name'
                 left join xml_tree_values xtv_source
                   on xtv_source.results_id = r.id and xtv_source.simple_path = 'VipObject.Source.VipId'
                 where r.complete is true
                   and r.spec_version like '5.1%'
                   and xtv_source.value is not null
                 group by xtv_source.value, xtv_state.value, r.election_type, r.election_date
            union all
                   select
                      states.name as state,
                      sources.vip_id as vip_id,
                      r.election_type as election_type,
                      r.election_date as election_date,
                      array_agg(r.id order by r.id desc) as ids
                    from results r
                    left join v3_0_sources sources on r.id = sources.results_id
                    left join v3_0_states states on r.id = states.results_id
                    where r.spec_version = '3.0'
                     and r.complete is true
                     and sources.vip_id is not null
                    group by sources.vip_id, states.name, r.election_type, r.election_date)
  select
    state,
    vip_id,
    ids[1:5] as current_runs,
    ids[6:array_upper(ids, 1)] as old_runs
  from feeds
  where cardinality(ids) > 5;
