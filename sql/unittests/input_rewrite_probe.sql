SELECT  /*+ no_parallel */  A, B, C, countA
FROM (
         SELECT  /*+ no_parallel */  A, B, C,
                                     count(A) OVER (PARTITION BY B ORDER BY A) countA
         FROM R_row_10
     ) T1
WHERE C < 1