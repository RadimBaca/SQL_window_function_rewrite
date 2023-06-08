
---------------------------------------------
--------------  Row store -------------------
---------------------------------------------

DROP TABLE IF EXISTS dbo.R_row_10
DROP TABLE IF EXISTS dbo.R_row_30
DROP TABLE IF EXISTS dbo.R_row_100
DROP TABLE IF EXISTS dbo.R_row_300
DROP TABLE IF EXISTS dbo.R_row_1000
DROP TABLE IF EXISTS dbo.R_row_3000
DROP TABLE IF EXISTS dbo.R_row_10000
DROP TABLE IF EXISTS dbo.R_row_30000
DROP TABLE IF EXISTS dbo.R_row_100000
DROP TABLE IF EXISTS dbo.R_row_300000
GO

WITH x AS
         (
             SELECT n FROM (VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) v(n)
         ), t1 AS
         (
             SELECT ones.n + 10 * tens.n + 100 * hundreds.n + 1000 * thousands.n + 10000 * tenthousands.n + 100000 * hundredthousands.n  as id
             FROM x ones,     x tens,      x hundreds,       x thousands,       x tenthousands,       x hundredthousands
         )
SELECT  id A,
        id % 10 AS B,
        ABS(CAST((CAST(SQRT(id + 1999999777) AS DECIMAL(38, 18)) - CAST(FLOOR(CAST(SQRT(id + 1999999777) AS DECIMAL(38, 18))) AS DECIMAL(38,18))) * 1000000000 AS INT)) % 10000 AS C
INTO R_row_10
FROM t1
GO

WITH x AS
         (
             SELECT n FROM (VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) v(n)
         ), t1 AS
         (
             SELECT ones.n + 10 * tens.n + 100 * hundreds.n + 1000 * thousands.n + 10000 * tenthousands.n + 100000 * hundredthousands.n  as id
             FROM x ones,     x tens,      x hundreds,       x thousands,       x tenthousands,       x hundredthousands
         )
SELECT id A,
       id % 30 B, ABS(CAST((CAST(SQRT(id + 1999999777) AS DECIMAL(38, 18)) - CAST(FLOOR(CAST(SQRT(id + 1999999777) AS DECIMAL(38, 18))) AS DECIMAL(38,18))) * 1000000000 AS INT)) % 10000 AS C
into R_row_30
FROM t1
GO

WITH x AS
         (
             SELECT n FROM (VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) v(n)
         ), t1 AS
         (
             SELECT ones.n + 10 * tens.n + 100 * hundreds.n + 1000 * thousands.n + 10000 * tenthousands.n + 100000 * hundredthousands.n  as id
             FROM x ones,     x tens,      x hundreds,       x thousands,       x tenthousands,       x hundredthousands
         )
SELECT  id A,
        id % 100 B, ABS(CAST((CAST(SQRT(id + 1999999777) AS DECIMAL(38, 18)) - CAST(FLOOR(CAST(SQRT(id + 1999999777) AS DECIMAL(38, 18))) AS DECIMAL(38,18))) * 1000000000 AS INT)) % 10000 AS C
into R_row_100
FROM t1
GO

WITH x AS
         (
             SELECT n FROM (VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) v(n)
         ), t1 AS
         (
             SELECT ones.n + 10 * tens.n + 100 * hundreds.n + 1000 * thousands.n + 10000 * tenthousands.n + 100000 * hundredthousands.n  as id
             FROM x ones,     x tens,      x hundreds,       x thousands,       x tenthousands,       x hundredthousands
         )
SELECT id A,
       id % 300 B, ABS(CAST((CAST(SQRT(id + 1999999777) AS DECIMAL(38, 18)) - CAST(FLOOR(CAST(SQRT(id + 1999999777) AS DECIMAL(38, 18))) AS DECIMAL(38,18))) * 1000000000 AS INT)) % 10000 AS C
into R_row_300
FROM t1
GO

WITH x AS
         (
             SELECT n FROM (VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) v(n)
         ), t1 AS
         (
             SELECT ones.n + 10 * tens.n + 100 * hundreds.n + 1000 * thousands.n + 10000 * tenthousands.n + 100000 * hundredthousands.n  as id
             FROM x ones,     x tens,      x hundreds,       x thousands,       x tenthousands,       x hundredthousands
         )
SELECT id A,
       id % 1000 B, ABS(CAST((CAST(SQRT(id + 1999999777) AS DECIMAL(38, 18)) - CAST(FLOOR(CAST(SQRT(id + 1999999777) AS DECIMAL(38, 18))) AS DECIMAL(38,18))) * 1000000000 AS INT)) % 10000 AS C into R_row_1000
FROM t1
GO

WITH x AS
         (
             SELECT n FROM (VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) v(n)
         ), t1 AS
         (
             SELECT ones.n + 10 * tens.n + 100 * hundreds.n + 1000 * thousands.n + 10000 * tenthousands.n + 100000 * hundredthousands.n  as id
             FROM x ones,     x tens,      x hundreds,       x thousands,       x tenthousands,       x hundredthousands
         )
SELECT id A,
       id % 3000 B, ABS(CAST((CAST(SQRT(id + 1999999777) AS DECIMAL(38, 18)) - CAST(FLOOR(CAST(SQRT(id + 1999999777) AS DECIMAL(38, 18))) AS DECIMAL(38,18))) * 1000000000 AS INT)) % 10000 AS C
into R_row_3000
FROM t1
GO

WITH x AS
         (
             SELECT n FROM (VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) v(n)
         ), t1 AS
         (
             SELECT ones.n + 10 * tens.n + 100 * hundreds.n + 1000 * thousands.n + 10000 * tenthousands.n + 100000 * hundredthousands.n  as id
             FROM x ones,     x tens,      x hundreds,       x thousands,       x tenthousands,       x hundredthousands
         )
SELECT id A,
       id % 10000 B, ABS(CAST((CAST(SQRT(id + 1999999777) AS DECIMAL(38, 18)) - CAST(FLOOR(CAST(SQRT(id + 1999999777) AS DECIMAL(38, 18))) AS DECIMAL(38,18))) * 1000000000 AS INT)) % 10000 AS C
into R_row_10000
FROM t1
GO

WITH x AS
         (
             SELECT n FROM (VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) v(n)
         ), t1 AS
         (
             SELECT ones.n + 10 * tens.n + 100 * hundreds.n + 1000 * thousands.n + 10000 * tenthousands.n + 100000 * hundredthousands.n  as id
             FROM x ones,     x tens,      x hundreds,       x thousands,       x tenthousands,       x hundredthousands
         )
SELECT id A,
       id % 30000 B, ABS(CAST((CAST(SQRT(id + 1999999777) AS DECIMAL(38, 18)) - CAST(FLOOR(CAST(SQRT(id + 1999999777) AS DECIMAL(38, 18))) AS DECIMAL(38,18))) * 1000000000 AS INT)) % 10000 AS C
into R_row_30000
FROM t1
GO

WITH x AS
         (
             SELECT n FROM (VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) v(n)
         ), t1 AS
         (
             SELECT ones.n + 10 * tens.n + 100 * hundreds.n + 1000 * thousands.n + 10000 * tenthousands.n + 100000 * hundredthousands.n  as id
             FROM x ones,     x tens,      x hundreds,       x thousands,       x tenthousands,       x hundredthousands
         )
SELECT id A,
       id % 100000 B, ABS(CAST((CAST(SQRT(id + 1999999777) AS DECIMAL(38, 18)) - CAST(FLOOR(CAST(SQRT(id + 1999999777) AS DECIMAL(38, 18))) AS DECIMAL(38,18))) * 1000000000 AS INT)) % 10000 AS C
into R_row_100000
FROM t1
GO

WITH x AS
         (
             SELECT n FROM (VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) v(n)
         ), t1 AS
         (
             SELECT ones.n + 10 * tens.n + 100 * hundreds.n + 1000 * thousands.n + 10000 * tenthousands.n + 100000 * hundredthousands.n  as id
             FROM x ones,     x tens,      x hundreds,       x thousands,       x tenthousands,       x hundredthousands
         )
SELECT id A,
       id % 300000 B, ABS(CAST((CAST(SQRT(id + 1999999777) AS DECIMAL(38, 18)) - CAST(FLOOR(CAST(SQRT(id + 1999999777) AS DECIMAL(38, 18))) AS DECIMAL(38,18))) * 1000000000 AS INT)) % 10000 AS C
into R_row_300000
FROM t1
GO


---------------------------------------------
-------------  Column store -----------------
---------------------------------------------

DROP TABLE IF EXISTS DBO.R_column_10
DROP TABLE IF EXISTS DBO.R_column_30
DROP TABLE IF EXISTS DBO.R_column_100
DROP TABLE IF EXISTS DBO.R_column_300
DROP TABLE IF EXISTS DBO.R_column_1000
DROP TABLE IF EXISTS DBO.R_column_3000
DROP TABLE IF EXISTS DBO.R_column_10000
DROP TABLE IF EXISTS DBO.R_column_30000
DROP TABLE IF EXISTS DBO.R_column_100000
DROP TABLE IF EXISTS DBO.R_column_300000
GO

SELECT * INTO R_column_10 FROM R_row_10
CREATE CLUSTERED COLUMNSTORE INDEX IDX_R_column_10 ON R_column_10

SELECT * INTO R_column_30 FROM R_row_30
CREATE CLUSTERED COLUMNSTORE INDEX IDX_R_column_30 ON R_column_30

SELECT * INTO R_column_100 FROM R_row_100
CREATE CLUSTERED COLUMNSTORE INDEX IDX_R_column_100 ON R_column_100

SELECT * INTO R_column_300 FROM R_row_300
CREATE CLUSTERED COLUMNSTORE INDEX IDX_R_column_300 ON R_column_300

SELECT * INTO R_column_1000 FROM R_row_1000
CREATE CLUSTERED COLUMNSTORE INDEX IDX_R_column_1000 ON R_column_1000

SELECT * INTO R_column_3000 FROM R_row_3000
CREATE CLUSTERED COLUMNSTORE INDEX IDX_R_column_3000 ON R_column_3000

SELECT * INTO R_column_10000 FROM R_row_10000
CREATE CLUSTERED COLUMNSTORE INDEX IDX_R_column_10000 ON R_column_10000

SELECT * INTO R_column_30000 FROM R_row_30000
CREATE CLUSTERED COLUMNSTORE INDEX IDX_R_column_30000 ON R_column_30000

SELECT * INTO R_column_100000 FROM R_row_100000
CREATE CLUSTERED COLUMNSTORE INDEX IDX_R_column_100000 ON R_column_100000

SELECT * INTO R_column_300000 FROM R_row_300000
CREATE CLUSTERED COLUMNSTORE INDEX IDX_R_column_300000 ON R_column_300000




---------------------------------------------
--------------  Row store -------------------
---------------------------------------------

DROP TABLE IF EXISTS dbo.P_row_10
DROP TABLE IF EXISTS dbo.P_row_30
DROP TABLE IF EXISTS dbo.P_row_100
DROP TABLE IF EXISTS dbo.P_row_300
DROP TABLE IF EXISTS dbo.P_row_1000
DROP TABLE IF EXISTS dbo.P_row_3000
DROP TABLE IF EXISTS dbo.P_row_10000
DROP TABLE IF EXISTS dbo.P_row_30000
DROP TABLE IF EXISTS dbo.P_row_100000
DROP TABLE IF EXISTS dbo.P_row_300000
GO

WITH x AS
         (
             SELECT n FROM (VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) v(n)
         ), t1 AS
         (
             SELECT ones.n + 10 * tens.n + 100 * hundreds.n + 1000 * thousands.n + 10000 * tenthousands.n + 100000 * hundredthousands.n  as id
             FROM x ones,     x tens,      x hundreds,       x thousands,       x tenthousands,       x hundredthousands
         )
SELECT  id A,
        id % 10 B,
        LEFT('Value ' + CAST(id AS VARCHAR) + ' ' + REPLICATE('*', 100), 100) as padding,
        ABS(CAST((CAST(SQRT(id + 1999999777) AS DECIMAL(38, 18)) - CAST(FLOOR(CAST(SQRT(id + 1999999777) AS DECIMAL(38, 18))) AS DECIMAL(38,18))) * 1000000000 AS INT)) % 10000 AS C into P_row_10
FROM t1
GO

WITH x AS
         (
             SELECT n FROM (VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) v(n)
         ), t1 AS
         (
             SELECT ones.n + 10 * tens.n + 100 * hundreds.n + 1000 * thousands.n + 10000 * tenthousands.n + 100000 * hundredthousands.n  as id
             FROM x ones,     x tens,      x hundreds,       x thousands,       x tenthousands,       x hundredthousands
         )
SELECT id A,
       id % 30 B,
       LEFT('Value ' + CAST(id AS VARCHAR) + ' ' + REPLICATE('*', 100), 100) as padding,
       ABS(CAST((CAST(SQRT(id + 1999999777) AS DECIMAL(38, 18)) - CAST(FLOOR(CAST(SQRT(id + 1999999777) AS DECIMAL(38, 18))) AS DECIMAL(38,18))) * 1000000000 AS INT)) % 10000 AS C into P_row_30
FROM t1
GO

WITH x AS
         (
             SELECT n FROM (VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) v(n)
         ), t1 AS
         (
             SELECT ones.n + 10 * tens.n + 100 * hundreds.n + 1000 * thousands.n + 10000 * tenthousands.n + 100000 * hundredthousands.n  as id
             FROM x ones,     x tens,      x hundreds,       x thousands,       x tenthousands,       x hundredthousands
         )
SELECT  id A,
        id % 100 B,
        LEFT('Value ' + CAST(id AS VARCHAR) + ' ' + REPLICATE('*', 100), 100) as padding,
        ABS(CAST((CAST(SQRT(id + 1999999777) AS DECIMAL(38, 18)) - CAST(FLOOR(CAST(SQRT(id + 1999999777) AS DECIMAL(38, 18))) AS DECIMAL(38,18))) * 1000000000 AS INT)) % 10000 AS C into P_row_100
FROM t1
GO

WITH x AS
         (
             SELECT n FROM (VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) v(n)
         ), t1 AS
         (
             SELECT ones.n + 10 * tens.n + 100 * hundreds.n + 1000 * thousands.n + 10000 * tenthousands.n + 100000 * hundredthousands.n  as id
             FROM x ones,     x tens,      x hundreds,       x thousands,       x tenthousands,       x hundredthousands
         )
SELECT id A,
       id % 300 B,
       LEFT('Value ' + CAST(id AS VARCHAR) + ' ' + REPLICATE('*', 100), 100) as padding,
       ABS(CAST((CAST(SQRT(id + 1999999777) AS DECIMAL(38, 18)) - CAST(FLOOR(CAST(SQRT(id + 1999999777) AS DECIMAL(38, 18))) AS DECIMAL(38,18))) * 1000000000 AS INT)) % 10000 AS C into P_row_300
FROM t1
GO

WITH x AS
         (
             SELECT n FROM (VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) v(n)
         ), t1 AS
         (
             SELECT ones.n + 10 * tens.n + 100 * hundreds.n + 1000 * thousands.n + 10000 * tenthousands.n + 100000 * hundredthousands.n  as id
             FROM x ones,     x tens,      x hundreds,       x thousands,       x tenthousands,       x hundredthousands
         )
SELECT id A,
       id % 1000 B,
       LEFT('Value ' + CAST(id AS VARCHAR) + ' ' + REPLICATE('*', 100), 100) as padding,
       ABS(CAST((CAST(SQRT(id + 1999999777) AS DECIMAL(38, 18)) - CAST(FLOOR(CAST(SQRT(id + 1999999777) AS DECIMAL(38, 18))) AS DECIMAL(38,18))) * 1000000000 AS INT)) % 10000 AS C into P_row_1000
FROM t1
GO

WITH x AS
         (
             SELECT n FROM (VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) v(n)
         ), t1 AS
         (
             SELECT ones.n + 10 * tens.n + 100 * hundreds.n + 1000 * thousands.n + 10000 * tenthousands.n + 100000 * hundredthousands.n  as id
             FROM x ones,     x tens,      x hundreds,       x thousands,       x tenthousands,       x hundredthousands
         )
SELECT id A,
       id % 3000 B,
       LEFT('Value ' + CAST(id AS VARCHAR) + ' ' + REPLICATE('*', 100), 100) as padding,
       ABS(CAST((CAST(SQRT(id + 1999999777) AS DECIMAL(38, 18)) - CAST(FLOOR(CAST(SQRT(id + 1999999777) AS DECIMAL(38, 18))) AS DECIMAL(38,18))) * 1000000000 AS INT)) % 10000 AS C into P_row_3000
FROM t1
GO

WITH x AS
         (
             SELECT n FROM (VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) v(n)
         ), t1 AS
         (
             SELECT ones.n + 10 * tens.n + 100 * hundreds.n + 1000 * thousands.n + 10000 * tenthousands.n + 100000 * hundredthousands.n  as id
             FROM x ones,     x tens,      x hundreds,       x thousands,       x tenthousands,       x hundredthousands
         )
SELECT id A,
       id % 10000 B,
       LEFT('Value ' + CAST(id AS VARCHAR) + ' ' + REPLICATE('*', 100), 100) as padding,
       ABS(CAST((CAST(SQRT(id + 1999999777) AS DECIMAL(38, 18)) - CAST(FLOOR(CAST(SQRT(id + 1999999777) AS DECIMAL(38, 18))) AS DECIMAL(38,18))) * 1000000000 AS INT)) % 10000 AS C into P_row_10000
FROM t1
GO

WITH x AS
         (
             SELECT n FROM (VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) v(n)
         ), t1 AS
         (
             SELECT ones.n + 10 * tens.n + 100 * hundreds.n + 1000 * thousands.n + 10000 * tenthousands.n + 100000 * hundredthousands.n  as id
             FROM x ones,     x tens,      x hundreds,       x thousands,       x tenthousands,       x hundredthousands
         )
SELECT id A,
       id % 30000 B,
       LEFT('Value ' + CAST(id AS VARCHAR) + ' ' + REPLICATE('*', 100), 100) as padding,
       ABS(CAST((CAST(SQRT(id + 1999999777) AS DECIMAL(38, 18)) - CAST(FLOOR(CAST(SQRT(id + 1999999777) AS DECIMAL(38, 18))) AS DECIMAL(38,18))) * 1000000000 AS INT)) % 10000 AS C into P_row_30000
FROM t1
GO

WITH x AS
         (
             SELECT n FROM (VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) v(n)
         ), t1 AS
         (
             SELECT ones.n + 10 * tens.n + 100 * hundreds.n + 1000 * thousands.n + 10000 * tenthousands.n + 100000 * hundredthousands.n  as id
             FROM x ones,     x tens,      x hundreds,       x thousands,       x tenthousands,       x hundredthousands
         )
SELECT id A,
       id % 100000 B,
       LEFT('Value ' + CAST(id AS VARCHAR) + ' ' + REPLICATE('*', 100), 100) as padding,
       ABS(CAST((CAST(SQRT(id + 1999999777) AS DECIMAL(38, 18)) - CAST(FLOOR(CAST(SQRT(id + 1999999777) AS DECIMAL(38, 18))) AS DECIMAL(38,18))) * 1000000000 AS INT)) % 10000 AS C into P_row_100000
FROM t1
GO

WITH x AS
         (
             SELECT n FROM (VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) v(n)
         ), t1 AS
         (
             SELECT ones.n + 10 * tens.n + 100 * hundreds.n + 1000 * thousands.n + 10000 * tenthousands.n + 100000 * hundredthousands.n  as id
             FROM x ones,     x tens,      x hundreds,       x thousands,       x tenthousands,       x hundredthousands
         )
SELECT id A,
       id % 300000 B,
       LEFT('Value ' + CAST(id AS VARCHAR) + ' ' + REPLICATE('*', 100), 100) as padding,
       ABS(CAST((CAST(SQRT(id + 1999999777) AS DECIMAL(38, 18)) - CAST(FLOOR(CAST(SQRT(id + 1999999777) AS DECIMAL(38, 18))) AS DECIMAL(38,18))) * 1000000000 AS INT)) % 10000 AS C into P_row_300000
FROM t1
GO


---------------------------------------------
-------------  Column store -----------------
---------------------------------------------

DROP TABLE IF EXISTS DBO.P_column_10
DROP TABLE IF EXISTS DBO.P_column_30
DROP TABLE IF EXISTS DBO.P_column_100
DROP TABLE IF EXISTS DBO.P_column_300
DROP TABLE IF EXISTS DBO.P_column_1000
DROP TABLE IF EXISTS DBO.P_column_3000
DROP TABLE IF EXISTS DBO.P_column_10000
DROP TABLE IF EXISTS DBO.P_column_30000
DROP TABLE IF EXISTS DBO.P_column_100000
DROP TABLE IF EXISTS DBO.P_column_300000
GO

SELECT * INTO P_column_10 FROM P_row_10
CREATE CLUSTERED COLUMNSTORE INDEX IDX_P_column_10 ON P_column_10

SELECT * INTO P_column_30 FROM P_row_30
CREATE CLUSTERED COLUMNSTORE INDEX IDX_P_column_30 ON P_column_30

SELECT * INTO P_column_100 FROM P_row_100
CREATE CLUSTERED COLUMNSTORE INDEX IDX_P_column_100 ON P_column_100

SELECT * INTO P_column_300 FROM P_row_300
CREATE CLUSTERED COLUMNSTORE INDEX IDX_P_column_300 ON P_column_300

SELECT * INTO P_column_1000 FROM P_row_1000
CREATE CLUSTERED COLUMNSTORE INDEX IDX_P_column_1000 ON P_column_1000

SELECT * INTO P_column_3000 FROM P_row_3000
CREATE CLUSTERED COLUMNSTORE INDEX IDX_P_column_3000 ON P_column_3000

SELECT * INTO P_column_10000 FROM P_row_10000
CREATE CLUSTERED COLUMNSTORE INDEX IDX_P_column_10000 ON P_column_10000

SELECT * INTO P_column_30000 FROM P_row_30000
CREATE CLUSTERED COLUMNSTORE INDEX IDX_P_column_30000 ON P_column_30000

SELECT * INTO P_column_100000 FROM P_row_100000
CREATE CLUSTERED COLUMNSTORE INDEX IDX_P_column_100000 ON P_column_100000

SELECT * INTO P_column_300000 FROM P_row_300000
CREATE CLUSTERED COLUMNSTORE INDEX IDX_P_column_300000 ON P_column_300000



