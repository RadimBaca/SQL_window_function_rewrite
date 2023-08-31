min,PB_OB,<SSS
SELECT A, B, C, padding, minA
FROM (
         SELECT A, B, C, padding,
                min(A) OVER (PARTITION BY B ORDER BY A) minA
         FROM TAB
     ) T1
WHERE C < SSS