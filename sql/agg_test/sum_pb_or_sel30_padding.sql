sum,BP_OB,<8
SELECT A, B, C, padding, countA
FROM (
         SELECT A, B, C, padding,
                count(A) OVER (PARTITION BY B ORDER BY A) countA
         FROM TAB
     ) T1
WHERE C < 8