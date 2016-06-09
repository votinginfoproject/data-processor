ALTER TABLE v5_1_schedules
DROP COLUMN start_time2,
DROP COLUMN end_time2,
ADD COLUMN hours_open_id TEXT;

DROP TABLE IF EXISTS v5_1_hours_open;
