begin;
alter table results
  drop column state;
alter table results
  drop column election_type;
alter table results
  drop column election_date;
alter table results
  drop column vip_id;
commit;
