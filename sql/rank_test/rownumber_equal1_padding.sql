=1
SELECT A, B, PADDING
FROM (
         SELECT A, B, PADDING,
                ROW_NUMBER() OVER (PARTITION BY B ORDER BY A) RN
         FROM TAB
     ) T1
WHERE RN = 1