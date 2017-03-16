alter table v3_0_street_segments alter column start_apartment_number set data type integer using start_apartment_number::integer;
alter table v3_0_street_segments alter column end_apartment_number set data type integer using end_apartment_number::integer;
