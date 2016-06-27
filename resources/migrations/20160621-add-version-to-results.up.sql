ALTER TABLE results ADD COLUMN spec_version TEXT;

-- Set version numbers for existing rows using the heuristic: if the
-- result has a row in v3_0_sources, then it's a 3.0 feed, otherwise
-- it's a 5.1 feed.
UPDATE results
SET spec_version = versions.spec_version
FROM (SELECT results.id AS id,
             CASE WHEN v3_0_sources.results_id IS NULL
                  THEN '5.1'
                  ELSE '3.0'
             END AS spec_version
      FROM results
      LEFT JOIN v3_0_sources
             ON v3_0_sources.results_id = results.ID
      ) AS versions
WHERE results.id = versions.id;
