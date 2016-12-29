begin;
alter table results add column state text, add column election_type text, add column election_date text;

with t as
(select
   r.id as results_id,
   coalesce(e3.election_type, xtv.value, 'UNKNOWN') as type
   from results r
   left join v3_0_elections e3 on e3.results_id = r.id
   left join xml_tree_values xtv on xtv.results_id = r.id and xtv.path ~ 'VipObject.*.Election.*.ElectionType.*.Text.0'
),

s as
(select r.id as results_id,
 coalesce(s3.name, xtv_state.value, 'UNKNOWN') as state
from results r
left join v3_0_states s3 on s3.results_id = r.id
left join xml_tree_values xtv_state on xtv_state.results_id = r.id and xtv_state.simple_path = 'VipObject.State.Name'
),

d as
(select
   r.id as results_id,
   case when e3.date is not null then date(e3.date)
        when xtv_date.value is not null then date(xtv_date.value)
   end as date
 from results r
 left join v3_0_elections e3 on e3.results_id = r.id
 left join xml_tree_values xtv_date on xtv_date.results_id = r.id and xtv_date.simple_path = 'VipObject.Election.Date'),

v as
(select
  t.results_id,
   case when length(trim(both from t.type, ' ')) = 0 then 'UNKNOWN' else t.type end as election_type,
   s.state as state,
   d.date as date
 from t
 left join s on s.results_id = t.results_id
 left join d on d.results_id = t.results_id)
update results
set state = v.state, election_type = v.election_type, election_date = v.date
from v
where id = v.results_id;

commit;
--
--
--
-- "SELECT DISTINCT ON (r.id) \
--                  r.public_id, r.start_time, date(r.end_time) AS end_time, \
--                  CASE WHEN r.end_time IS NOT NULL \
--                       THEN r.end_time - r.start_time END AS duration, \
--                  r.spec_version, r.complete, COALESCE(s3.name, xtv_state.value) AS state, \
--                  COALESCE(e3.election_type, xtv_type.value) AS election_type, \
--                  CASE WHEN e3.date IS NOT NULL THEN DATE(e3.date) \
--                       WHEN xtv_date.value IS NOT NULL THEN DATE(xtv_date.value) END \
--                       AS election_date \
--           FROM results r \
--           LEFT JOIN v3_0_states s3 ON s3.results_id = r.id \
--           LEFT JOIN v3_0_elections e3 ON e3.results_id = r.id \
--           LEFT JOIN xml_tree_values xtv_state ON xtv_state.results_id = r.id \
--                                              AND xtv_state.simple_path = 'VipObject.State.Name' \
--           LEFT JOIN xml_tree_values xtv_type ON xtv_type.results_id = r.id \
--                                             AND xtv_type.path ~ 'VipObject.*.Election.*.ElectionType.*.Text.0' \
--           LEFT JOIN xml_tree_values xtv_date ON xtv_date.results_id = r.id \
--                                             AND xtv_date.simple_path = 'VipObject.Election.Date' \
--           ORDER BY r.id DESC \
--           LIMIT 20 OFFSET ($1 * 20);",
