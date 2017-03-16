CREATE OR REPLACE FUNCTION element_type(path ltree)
RETURNS varchar
AS $$

DECLARE
element_type varchar;

BEGIN
  element_type := CASE
    WHEN path ~ 'VipObject.0.ElectionAdministration.*{1}.Department.*{1}.VoterService.*'
    THEN 'VoterService'

    WHEN path ~ 'VipObject.0.ElectionAdministration.*{1}.Department.*'
    THEN 'Department'

    WHEN path ~ 'VipObject.0.ElectionAdministration.*'
    THEN 'ElectionAdministration'

    WHEN nlevel(path) > 2
    THEN ltree2text(subltree(path, 2, 3))

    ELSE NULL
   END;

   RETURN element_type;
END;
$$ LANGUAGE plpgsql;
