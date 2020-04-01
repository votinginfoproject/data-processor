CREATE TABLE public.xml_table_truncate_runs (time_of_run TIMESTAMP WITH TIME ZONE,
    																				 xml_table_size BIGINT,
    																		 		 xml_table_size_pretty TEXT,
																						 db_size BIGINT,
																						 db_size_pretty TEXT);

CREATE OR REPLACE FUNCTION clear_xml_table() RETURNS trigger AS $$
	BEGIN
		LOCK TABLE public.results IN EXCLUSIVE MODE;
		IF (SELECT COUNT(*) FROM public.results WHERE complete = false) = 0 THEN
			INSERT INTO public.xml_table_truncate_runs(
				time_of_run,
				xml_table_size,
				xml_table_size_pretty,
				db_size,
				db_size_pretty)
				VALUES(
					(SELECT NOW()),
					(SELECT PG_TOTAL_RELATION_SIZE('public.xml_tree_values')),
					(SELECT PG_SIZE_PRETTY(PG_TOTAL_RELATION_SIZE('public.xml_tree_values'))),
					(SELECT PG_DATABASE_SIZE('dataprocessor')),
					(SELECT PG_SIZE_PRETTY(PG_DATABASE_SIZE('dataprocessor'))));
			TRUNCATE TABLE public.xml_tree_values;
		END IF;
		RETURN null;
	END; $$
LANGUAGE PLPGSQL;

CREATE TRIGGER all_feeds_complete
	AFTER UPDATE ON public.results
	EXECUTE PROCEDURE clear_xml_table();
