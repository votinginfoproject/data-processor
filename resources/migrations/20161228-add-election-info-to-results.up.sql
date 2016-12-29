begin;
alter table results add column state text, add column election_type text, add column election_date text;

with new_values
  as (select r.id as results_id,
        coalesce(s3.name, xtv_state.value, 'UNKNOWN') as state,
        coalesce(e3.election_type, xtv_type.value, 'UNKNOWN') as election_type,
        case when e3.date is not null then date(e3.date)
             when xtv_date.value is not null then date(xtv_date.value) end
             as election_date
      from results r
      left join v3_0_states s3 on s3.results_id = r.id
      left join xml_tree_values xtv_state on xtv_state.results_id = r.id and xtv_state.simple_path = 'VipObject.State.Name'
      left join v3_0_elections e3 on e3.results_id = r.id
      left join xml_tree_values xtv_type on xtv_type.results_id = r.id and xtv_type.path ~ 'VipObject.*.Election.*.ElectionType.*.Text.0'
      left join xml_tree_values xtv_date on xtv_date.results_id = r.id and xtv_date.simple_path = 'VipObject.Election.Date'
      )
update results
set state = new_values.state, election_type = new_values.election_type, election_date = new_values.election_date
from new_values
where id = new_values.results_id;

commit;
