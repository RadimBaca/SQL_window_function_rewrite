SELECT  A, B, C, countA
FROM (
         SELECT  A, B, C,
                                     count(A) OVER (PARTITION BY B) countA
         FROM R_row_10
     ) T1
WHERE C < 1