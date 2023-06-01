sum,BP_OB,<16
SELECT A, B, C, padding, countA
FROM (
         SELECT A, B, C, padding,
                count(A) OVER (PARTITION BY B ORDER BY A) countA
         FROM TAB
     ) T1
WHERE C < 16