WITH test AS (
	SELECT 1 AS a, 'one' as b
	UNION ALL
	SELECT 2 AS a, 'two' as b
	UNION ALL
	SELECT 4 AS a, 'three' as b
)
SELECT
  *
FROM
  test
WHERE 
  a = @a AND b = @b
