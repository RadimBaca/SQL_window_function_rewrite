
SELECT A, B, C, minA
FROM (
         SELECT A, B, C,
                min(A) OVER (PARTITION BY B ORDER BY A) minA
         FROM R_ROW_1000
     ) T1
WHERE C < 1
