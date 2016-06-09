ALTER TABLE v5_1_schedules
ADD COLUMN start_time2 TEXT,
ADD COLUMN end_time2 TEXT,
DROP COLUMN hours_open_id;

CREATE TABLE v5_1_hours_open (id TEXT NOT NULL,
                              results_id BIGINT REFERENCES results(id) NOT NULL,
                              schedule_id TEXT NOT NULL,
                              PRIMARY KEY (results_id, id, schedule_id));
