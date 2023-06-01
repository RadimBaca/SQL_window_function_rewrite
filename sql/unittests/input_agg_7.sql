SELECT ID, GROUPBY
FROM (
         SELECT ID, GROUPBY,
                ROW_NUMBER() OVER (PARTITION BY GROUPBY ORDER BY  ID) RD1
         FROM A
         WHERE GROUPBY IS NULL OR GROUPBY > 90 AND ID IS NOT NULL
     ) T1
WHERE RD1 = 1
UNION ALL
SELECT ID, GROUPBY
FROM (
    SELECT GROUPBY, SEARCH AS ID,
           ROW_NUMBER() OVER (PARTITION BY GROUPBY ORDER BY ORDERBY, SEARCH) RD1
    FROM (
          SELECT DISTINCT GROUPBY, ORDERBY, SEARCH
            FROM A
            WHERE GROUPBY IS NULL OR GROUPBY > 90 AND SEARCH IS NOT NULL and ORDERBY IS NOT NULL
    ) T
) T2
WHERE RD1 = 5
UNION ALL
SELECT ID, GROUPBY
FROM (
         SELECT GROUPBY, SEARCH AS ID,
                ROW_NUMBER() OVER (PARTITION BY GROUPBY ORDER BY SEARCH) RD1
         FROM (
                  SELECT DISTINCT GROUPBY, SEARCH
                  FROM A
                  WHERE GROUPBY IS NULL OR GROUPBY > 90 AND SEARCH IS NOT NULL and ORDERBY IS NOT NULL
              ) T
     ) T2
WHERE RD1 < 5
ORDER BY ID, GROUPBY