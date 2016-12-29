begin;
alter table results add column state text, add column election_type text, add column election_date text;

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
 case when length(trim(both from t.type, ' ')) = 0 then 'UNKNOWN' else t.type end as election_type
from results r
left join v3_0_states s3 on s3.results_id = r.id
left join xml_tree_values xtv_state on xtv_state.results_id = r.id and xtv_state.simple_path = 'VipObject.State.Name'
left join v3_0_elections e3 on e3.results_id = r.id
left join xml_tree_values xtv_date on xtv_date.results_id = r.id and xtv_date.simple_path = 'VipObject.Election.Date'
left join type t on r.id = t.results_id
)

update results
set state = s.state, election_type = s.election_type, election_date = s.date
from state_date as s
where id = s.results_id;

commit;
