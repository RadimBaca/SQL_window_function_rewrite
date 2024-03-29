SELECT A.GROUPBY, A.SEARCH / CR, A.ORDERBY, A.ID, A.FKB, T1.AVG_FKB, T2.CR
FROM A
         JOIN (
    SELECT ID, GROUPBY, ORDERBY, SEARCH,
           AVG(FKB) OVER (PARTITION BY GROUPBY ORDER BY ORDERBY, SEARCH RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AVG_FKB
    FROM A
    WHERE GROUPBY > 93
) T1 ON A.ID = T1.ID
         JOIN (
    SELECT ID, COUNT(*) OVER (PARTITION BY GROUPBY ORDER BY ORDERBY, SEARCH RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) CR
    FROM A
    WHERE GROUPBY > 95
) T2 ON A.ID = T2.ID
WHERE (T1.SEARCH < 5 OR T1.SEARCH IS NULL)
ORDER BY A.GROUPBY, A.SEARCH, A.ORDERBY, A.ID