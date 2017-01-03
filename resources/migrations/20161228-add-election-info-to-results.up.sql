begin;
alter table results add column state text, add column election_type text, add column election_date text, add column vip_id text;

with type as
(select
   r.id as results_id,
   coalesce(e3.election_type, xtv.value, 'UNKNOWN') as type
   from results r
   left join v3_0_elections e3 on e3.results_id = r.id
   left join xml_tree_values xtv on xtv.results_id = r.id and xtv.path ~ 'VipObject.*.Election.*.ElectionType.*.Text.0'
),

state_date as
(select r.id as results_id,
 coalesce(s3.name, xtv_state.value, 'UNKNOWN') as state,
 case when e3.date is not null then date(e3.date)
      when xtv_date.value is not null then date(xtv_date.value) end as date,
 case when length(trim(both from t.type, ' ')) = 0 then 'UNKNOWN' else t.type end as election_type,
 coalesce(source3.vip_id, xtv_vip_id.value) as vip_id
from results r
left join v3_0_states s3 on s3.results_id = r.id
left join xml_tree_values xtv_state on xtv_state.results_id = r.id and xtv_state.simple_path = 'VipObject.State.Name'
left join v3_0_elections e3 on e3.results_id = r.id
left join xml_tree_values xtv_date on xtv_date.results_id = r.id and xtv_date.simple_path = 'VipObject.Election.Date'
left join type t on r.id = t.results_id
left join v3_0_sources source3 on source3.results_id = r.id
left join xml_tree_values xtv_vip_id on xtv_vip_id.results_id = r.id and xtv_vip_id.simple_path = 'VipObject.Source.VipId'
)

update results
set state = s.state, election_type = s.election_type, election_date = s.date, vip_id = s.vip_id
from state_date as s
where id = s.results_id;

commit;
