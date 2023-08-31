SELECT A, B, C, countA
FROM (
         SELECT A, B, C,
                count(A) OVER (PARTITION BY B ORDER BY A) countA
         FROM P_row_100
     ) T1
WHERE C < 32