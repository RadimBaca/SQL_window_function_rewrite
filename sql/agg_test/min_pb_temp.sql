min,PB,<SSS
SELECT A, B, C, minA
FROM (
         SELECT A, B, C,
                min(A) OVER (PARTITION BY B) minA
         FROM TAB
     ) T1
WHERE C < SSS