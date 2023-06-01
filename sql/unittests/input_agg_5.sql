SELECT ID, GROUPBY
FROM (
         SELECT ID, GROUPBY,
                RANK() OVER (PARTITION BY GROUPBY ORDER BY ORDERBY, SEARCH, ID) RD1,
                RANK() OVER (PARTITION BY GROUPBY ORDER BY ID) RD2,
                RANK() OVER (PARTITION BY GROUPBY ORDER BY ID) RD3
         FROM A
         WHERE GROUPBY IS NULL OR GROUPBY > 90 AND ID IS NOT NULL
     ) T1
WHERE RD1 < 30 AND (RD2 = 1 OR RD3 = 5)
ORDER BY GROUPBY, ID