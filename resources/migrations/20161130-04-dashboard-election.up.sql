create materialized view dashboard.elections
  (results_id, id, date, election_type, is_statewide, name, registration_info,
   absentee_ballot_info, results_uri, polling_hours, hours_open_id,
   has_election_day_registration, registration_deadline, absentee_request_deadline)
as
with
  texts as (
    select *
    from crosstab('select results_id, element_type(simple_path) as element, value
                  from i18n_text
                  where i18n_text.simple_path ~
                      ''VipObject.Election.AbsenteeBallotInfo|ElectionType|Name|RegistrationInfo.Text''
                  order by 1',
                  'select unnest(ARRAY[''AbsenteeBallotInfo'',
                                       ''ElectionType'',
                                       ''Name'',
                                       ''RegistrationInfo''])')
    as ct(results_id int, absentee_ballot_info text, election_type text,
          name text, registration_info text)),
  election as (
    select *
    from crosstab('select
                     xtv.results_id,
                     element_type(xtv.simple_path) as element,
                     xtv.value as value
                   from xml_tree_values xtv
                   where xtv.simple_path ~ ''VipObject.Election.*''
                   order by 1',
                  'select unnest(ARRAY[''AbsenteeRequestDeadline'',
                                       ''Date'',
                                       ''HasElectionDayRegistration'',
                                       ''IsStatewide'',
                                       ''PollingHours'',
                                       ''HoursOpenId'',
                                       ''RegistrationDeadline'',
                                       ''ResultsUri'',
                                       ''StateId'',
                                       ''id''])')
    as ct(results_id int, absentee_request_deadline text, date text,
          has_election_day_registration text, is_statewide text,
          polling_hours text, hours_open_id text, registration_deadline text,
          results_uri text, state_id text, id text))
select
  e.results_id, e.id, e.date, t.election_type, e.is_statewide, t.name,
  t.registration_info, t.absentee_ballot_info, e.results_uri, e.polling_hours,
  e.hours_open_id, e.has_election_day_registration, e.registration_deadline,
  e.absentee_request_deadline
from election e
left join texts t on t.results_id = e.results_id;
