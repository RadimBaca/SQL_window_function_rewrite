min,PB_OB,<SSS
SELECT A, B, C, minA
FROM (
         SELECT A, B, C,
                min(A) OVER (PARTITION BY B ORDER BY A) minA
         FROM TAB
     ) T1
WHERE C < SSS