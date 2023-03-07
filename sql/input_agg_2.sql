SELECT A.GROUPBY, A.SEARCH, A.ORDERBY, A.ID, A.FKB
FROM A
JOIN (
    SELECT ID, GROUPBY, ORDERBY, SEARCH,
           AVG(FKB) OVER (PARTITION BY GROUPBY ORDER BY ORDERBY, SEARCH) MIN_FKB
    FROM A
    WHERE GROUPBY > 97
) T ON A.ID = T.ID
WHERE A.FKB > T.MIN_FKB AND (T.SEARCH < 5 OR T.SEARCH IS NULL)
ORDER BY A.GROUPBY, A.SEARCH, A.ORDERBY, A.ID