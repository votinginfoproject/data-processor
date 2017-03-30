create or replace function v5_dashboard.populate_i18n_table(rid integer)
returns void as $$

with i18n as
  (select results_id, subpath(path, 0, nlevel(path) - 1) as path
   from xml_tree_values
   where simple_path ~ 'VipObject.*.Text.language'
     and value = 'en'
     and results_id = rid
   order by 1)
insert into v5_dashboard.i18n
  select
    distinct on (xtv.simple_path)
    xtv.results_id, xtv.simple_path, xtv.value
  from i18n
  left join xml_tree_values xtv
         on i18n.results_id = xtv.results_id
        and xtv.simple_path ~ 'VipObject.*.Text'
        and  i18n.path <@ xtv.path;
$$ language sql;

create or replace function v5_dashboard.populate_sources_table(rid integer)
returns void as $$

with
  texts as (
    select *
    from crosstab('select results_id, element_type(simple_path) as element, value
                  from v5_dashboard.i18n t
                  where t.simple_path ~
                      ''VipObject.Source.Description.Text''
                      and t.results_id = ' || rid || '
                  order by 1',
                  'select ''Description''')
    as ct(results_id int, description text)),

  source as (
    select *
    from crosstab('select
                     results_id,
                     element_type(simple_path) as element,
                     value
                   from xml_tree_values
                   where simple_path ~ ''VipObject.Source.!Text''
                   and results_id = ' || rid || '
                   order by 1',
                  'select unnest(ARRAY[''Name'',
                                       ''DateTime'',
                                       ''OrganizationUri'',
                                       ''TermsOfUseUri'',
                                       ''VipId'',
                                       ''id''])')
    as ct(results_id int, name text, datetime text, organization_uri text,
          terms_of_use_uri text, vip_id text, id text))
insert into v5_dashboard.sources
  select
    s.results_id, s.id, s.vip_id, s.name, s.datetime,
    t.description, s.organization_uri, s.terms_of_use_uri
  from source s
  left join texts t on t.results_id = s.results_id;
$$ language sql;

create or replace function v5_dashboard.populate_elections_table(rid integer)
returns void as $$

with
  texts as (
    select *
    from crosstab('select results_id, element_type(simple_path) as element, value
                  from v5_dashboard.i18n
                  where simple_path ~
                      ''VipObject.Election.AbsenteeBallotInfo|ElectionType|Name|RegistrationInfo.Text''
                  and results_id = '|| rid ||'
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
                   and xtv.results_id = '|| rid ||'
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
insert into v5_dashboard.elections
  select
    e.results_id, e.id, e.date, t.election_type, e.is_statewide, t.name,
    t.registration_info, t.absentee_ballot_info, e.results_uri, e.polling_hours,
    e.hours_open_id, e.has_election_day_registration, e.registration_deadline,
    e.absentee_request_deadline
  from election e
  left join texts t on t.results_id = e.results_id;

$$ language sql;
