alter table v5_statistics add column db_polling_location_count INTEGER DEFAULT 0;
alter table v5_statistics add column db_polling_location_errors INTEGER DEFAULT 0;
alter table v5_statistics add column db_polling_location_completion INTEGER DEFAULT 0;
alter table v5_statistics add column ev_polling_location_count INTEGER DEFAULT 0;
alter table v5_statistics add column ev_polling_location_errors INTEGER DEFAULT 0;
alter table v5_statistics add column ev_polling_location_completion INTEGER DEFAULT 0;
