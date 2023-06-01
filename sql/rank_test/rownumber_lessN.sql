SELECT A, B
FROM (
         SELECT A, B,
                ROW_NUMBER() OVER (PARTITION BY B ORDER BY A) RN
         FROM TAB
     ) T1
WHERE RN < 10
