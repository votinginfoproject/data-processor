CREATE TABLE results (id serial PRIMARY KEY,
                      start_time timestamp with time zone,
                      filename character varying(255),
                      complete boolean,
                      end_time timestamp with time zone);
