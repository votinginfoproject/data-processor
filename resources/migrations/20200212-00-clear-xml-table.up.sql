CREATE FUNCTION clear_xml_table() RETURNS trigger AS $$
	BEGIN
		LOCK TABLE public.results;
		IF (SELECT COUNT(*) FROM public.results WHERE complete = false) = 0 THEN
			TRUNCATE TABLE public.xml_tree_values;
		END IF;
		RETURN null;
	END; $$
LANGUAGE PLPGSQL;

CREATE TRIGGER all_feeds_complete
	AFTER UPDATE ON public.results
	EXECUTE PROCEDURE clear_xml_table();
