create or replace function convert_to_integer(apt_number_txt text)
returns integer as $$
declare apt_number_int integer default null;
begin
    begin
        apt_number_int := apt_number_txt::integer;
    exception when others then
        return null;
    end;
return apt_number_int;
end;
$$ language plpgsql;


alter table v3_0_street_segments alter column start_apartment_number set data type integer using convert_to_integer(start_apartment_number);
alter table v3_0_street_segments alter column end_apartment_number set data type integer using convert_to_integer(end_apartment_number);

drop function convert_to_integer(text);
