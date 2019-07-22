WITH test AS (
	SELECT 1 AS a, 'one' AS b, STRUCT('uno' AS c, 'ek' AS d) AS e
	UNION ALL
	SELECT 2 AS a, 'two' AS b, STRUCT('duos' AS c, 'don' AS d) AS e
	UNION ALL
	SELECT 4 AS a, 'three' AS b, STRUCT('tres' AS c, 'teen' AS d) AS e
)
SELECT
  *
FROM
  test
WHERE 
  e = @e
