count,PB,<SSS
SELECT A, B, C, countA
FROM (
         SELECT A, B, C,
                count(A) OVER (PARTITION BY B) countA
         FROM TAB
     ) T1
WHERE C < SSS