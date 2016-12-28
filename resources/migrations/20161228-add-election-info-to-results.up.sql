begin;
alter table results
  add column state text;
alter table results
  add column election_type text;
alter table results
  add column election_date text;
commit;
