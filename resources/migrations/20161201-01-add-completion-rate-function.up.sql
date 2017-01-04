create or replace function v5_dashboard.completion_rate(row_count bigint, error_count bigint)
returns int as $$
declare
completion int;
begin
  case
    when error_count > row_count then
      completion := 0;
    when row_count = 0 then
      completion := 100;
    else
      completion := floor(((row_count - error_count) / row_count::float) * 100);
  end case;
  return completion;
end;
$$ language plpgsql;
