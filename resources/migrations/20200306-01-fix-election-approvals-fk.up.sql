alter table election_approvals drop constraint election_approvals_approved_result_id_fkey;
alter table election_approvals add constraint election_approvals_approved_result_id_fkey foreign key (approved_result_id) references results(id) ON DELETE SET NULL;
