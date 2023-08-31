min,PB,<SSS
SELECT A, B, C, padding, minA
FROM (
         SELECT A, B, C, padding,
                min(A) OVER (PARTITION BY B) minA
         FROM TAB
     ) T1
WHERE C < SSS