SELECT A, B
FROM (
         SELECT A, B,
                ROW_NUMBER() OVER (PARTITION BY B ORDER BY A) RN
         FROM R_row_100
     ) T1
WHERE RN = 1