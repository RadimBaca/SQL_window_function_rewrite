count,BP,<SSS
SELECT A, B, C, countA, padding
FROM (
         SELECT A, B, C, padding,
                count(A) OVER (PARTITION BY B) countA
         FROM TAB
     ) T1
WHERE C < SSS