CREATE OR REPLACE FUNCTION countable_path(path ltree) RETURNS ltree AS $$
DECLARE
  subtree ltree;
BEGIN
  subtree := CASE
    WHEN path ~ 'VipObject.0.ElectionAdministration.*{1}.Department.*{1}.VoterService.*'
    THEN subltree(path, 0, 8)

    WHEN path ~ 'VipObject.0.ElectionAdministration.*{1}.Department.*'
    THEN subltree(path, 0, 6)

    ELSE subltree(path, 0, 4)
   END;

   RETURN subtree;
END;
$$ LANGUAGE plpgsql;
