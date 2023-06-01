sum,BP_OB,<4
SELECT A, B, C, countA
FROM (
         SELECT A, B, C,
                count(A) OVER (PARTITION BY B ORDER BY A) countA
         FROM TAB
     ) T1
WHERE C < 4