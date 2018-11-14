alter table validations add foreign key (results_id) references results(id) on delete cascade;
