drop materialized view if exists v5_dashboard.sources;


create table v5_dashboard.sources(
    results_id int references results (id) on delete cascade,
    id text,
    vip_id text,
    name text,
    datetime text,
    description text,
    organization_uri text,
    terms_of_use_uri text);

with
  texts as (
    select *
    from crosstab('select results_id, element_type(simple_path) as element, value
                  from v5_dashboard.i18n t
                  where t.simple_path ~
                      ''VipObject.Source.Description.Text''
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
