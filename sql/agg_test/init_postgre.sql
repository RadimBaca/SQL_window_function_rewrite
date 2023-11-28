-- Author : Radim Baca
-- Institution: VSB-Technical University of Ostrava
-- Database system tested: PostgreSQL 14.2
-- DDL script that prepares a PostgreSQL database for the microbenchmarks


---------------------------------------------
--------------  Row store -------------------
---------------------------------------------


DROP TABLE IF EXISTS "R_ROW_10" CASCADE;
DROP TABLE IF EXISTS "R_ROW_30" CASCADE;
DROP TABLE IF EXISTS "R_ROW_100" CASCADE;
DROP TABLE IF EXISTS "R_ROW_300" CASCADE;
DROP TABLE IF EXISTS "R_ROW_1000" CASCADE;
DROP TABLE IF EXISTS "R_ROW_3000" CASCADE;
DROP TABLE IF EXISTS "R_ROW_10000" CASCADE;
DROP TABLE IF EXISTS "R_ROW_30000" CASCADE;


CREATE TABLE "R_ROW_10" (
                            a int,
                            b int,
                            c int
);

INSERT INTO "R_ROW_10"
    WITH t1 AS
    (
SELECT id
FROM generate_series(0, 999999) id
    )
SELECT  id,
        id % 10 groupby,
        ABS(CAST((CAST(SQRT(id + 1999999777) AS DECIMAL(38, 18)) - CAST(FLOOR(CAST(SQRT(id + 1999999777) AS DECIMAL(38, 18))) AS DECIMAL(38,18))) * 1000000000 AS INT)) % 10000
FROM t1;

CREATE TABLE "R_ROW_30" (
                            a int,
                            b int,
                            c int
);

INSERT INTO "R_ROW_30"
    WITH t1 AS
    (
SELECT id
FROM generate_series(0, 999999) id
    )
SELECT  id,
        id % 30 groupby,
        ABS(CAST((CAST(SQRT(id + 1999999777) AS DECIMAL(38, 18)) - CAST(FLOOR(CAST(SQRT(id + 1999999777) AS DECIMAL(38, 18))) AS DECIMAL(38,18))) * 1000000000 AS INT)) % 10000
FROM t1;


CREATE TABLE "R_ROW_100" (
                             a int,
                             b int,
                             c int
);

INSERT INTO "R_ROW_100"
    WITH t1 AS
    (
SELECT id
FROM generate_series(0, 999999) id
    )
SELECT  id,
        id % 100 groupby,
        ABS(CAST((CAST(SQRT(id + 1999999777) AS DECIMAL(38, 18)) - CAST(FLOOR(CAST(SQRT(id + 1999999777) AS DECIMAL(38, 18))) AS DECIMAL(38,18))) * 1000000000 AS INT)) % 10000
FROM t1;


CREATE TABLE "R_ROW_300" (
                             a int,
                             b int,
                             c int
);

INSERT INTO "R_ROW_300"
    WITH t1 AS
    (
SELECT id
FROM generate_series(0, 999999) id
    )
SELECT  id,
        id % 300 groupby,
        ABS(CAST((CAST(SQRT(id + 1999999777) AS DECIMAL(38, 18)) - CAST(FLOOR(CAST(SQRT(id + 1999999777) AS DECIMAL(38, 18))) AS DECIMAL(38,18))) * 1000000000 AS INT)) % 10000
FROM t1;


CREATE TABLE "R_ROW_1000" (
                              a int,
                              b int,
                              c int
);

INSERT INTO "R_ROW_1000"
    WITH t1 AS
    (
SELECT id
FROM generate_series(0, 999999) id
    )
SELECT  id,
        id % 1000 groupby,
        ABS(CAST((CAST(SQRT(id + 1999999777) AS DECIMAL(38, 18)) - CAST(FLOOR(CAST(SQRT(id + 1999999777) AS DECIMAL(38, 18))) AS DECIMAL(38,18))) * 1000000000 AS INT)) % 10000
FROM t1;


CREATE TABLE "R_ROW_3000" (
                              a int,
                              b int,
                              c int
);

INSERT INTO "R_ROW_3000"
    WITH t1 AS
    (
SELECT id
FROM generate_series(0, 999999) id
    )
SELECT  id,
        id % 3000 groupby,
        ABS(CAST((CAST(SQRT(id + 1999999777) AS DECIMAL(38, 18)) - CAST(FLOOR(CAST(SQRT(id + 1999999777) AS DECIMAL(38, 18))) AS DECIMAL(38,18))) * 1000000000 AS INT)) % 10000
FROM t1;


CREATE TABLE "R_ROW_10000" (
                               a int,
                               b int,
                               c int
);

INSERT INTO "R_ROW_10000"
    WITH t1 AS
    (
SELECT id
FROM generate_series(0, 999999) id
    )
SELECT  id,
        id % 10000 groupby,
        ABS(CAST((CAST(SQRT(id + 1999999777) AS DECIMAL(38, 18)) - CAST(FLOOR(CAST(SQRT(id + 1999999777) AS DECIMAL(38, 18))) AS DECIMAL(38,18))) * 1000000000 AS INT)) % 10000
FROM t1;


CREATE TABLE "R_ROW_30000" (
                               a int,
                               b int,
                               c int
);

INSERT INTO "R_ROW_30000"
    WITH t1 AS
    (
SELECT id
FROM generate_series(0, 999999) id
    )
SELECT  id,
        id % 30000 groupby,
        ABS(CAST((CAST(SQRT(id + 1999999777) AS DECIMAL(38, 18)) - CAST(FLOOR(CAST(SQRT(id + 1999999777) AS DECIMAL(38, 18))) AS DECIMAL(38,18))) * 1000000000 AS INT)) % 10000
FROM t1;

-------------------------------------
-- Create the tables with padding

DROP TABLE IF EXISTS "P_ROW_10" CASCADE;
DROP TABLE IF EXISTS "P_ROW_30" CASCADE;
DROP TABLE IF EXISTS "P_ROW_100" CASCADE;
DROP TABLE IF EXISTS "P_ROW_300" CASCADE;
DROP TABLE IF EXISTS "P_ROW_1000" CASCADE;
DROP TABLE IF EXISTS "P_ROW_3000" CASCADE;
DROP TABLE IF EXISTS "P_ROW_10000" CASCADE;
DROP TABLE IF EXISTS "P_ROW_30000" CASCADE;


CREATE TABLE "P_ROW_10" (
                            a int,
                            b int,
                            c int,
                            padding char(200)
);

INSERT INTO "P_ROW_10"
    WITH t1 AS
    (
SELECT id
FROM generate_series(0, 999999) id
    )
SELECT  id,
        id % 10 groupby,
        ABS(CAST((CAST(SQRT(id + 1999999777) AS DECIMAL(38, 18)) - CAST(FLOOR(CAST(SQRT(id + 1999999777) AS DECIMAL(38, 18))) AS DECIMAL(38,18))) * 1000000000 AS INT)) % 10000,
        RPAD('Value ' || id || ' ' , 100, '*') as padding
FROM t1;

CREATE TABLE "P_ROW_30" (
                            a int,
                            b int,
                            c int,
                            padding char(200)
);

INSERT INTO "P_ROW_30"
    WITH t1 AS
    (
SELECT id
FROM generate_series(0, 999999) id
    )
SELECT  id,
        id % 30 groupby,
        ABS(CAST((CAST(SQRT(id + 1999999777) AS DECIMAL(38, 18)) - CAST(FLOOR(CAST(SQRT(id + 1999999777) AS DECIMAL(38, 18))) AS DECIMAL(38,18))) * 1000000000 AS INT)) % 10000,
        RPAD('Value ' || id || ' ' , 100, '*') as padding
FROM t1;


CREATE TABLE "P_ROW_100" (
                             a int,
                             b int,
                             c int,
                             padding char(200)
);

INSERT INTO "P_ROW_100"
    WITH t1 AS
    (
SELECT id
FROM generate_series(0, 999999) id
    )
SELECT  id,
        id % 100 groupby,
        ABS(CAST((CAST(SQRT(id + 1999999777) AS DECIMAL(38, 18)) - CAST(FLOOR(CAST(SQRT(id + 1999999777) AS DECIMAL(38, 18))) AS DECIMAL(38,18))) * 1000000000 AS INT)) % 10000,
        RPAD('Value ' || id || ' ' , 100, '*') as padding
FROM t1;


CREATE TABLE "P_ROW_300" (
                             a int,
                             b int,
                             c int,
                             padding char(200)
);

INSERT INTO "P_ROW_300"
    WITH t1 AS
    (
SELECT id
FROM generate_series(0, 999999) id
    )
SELECT  id,
        id % 300 groupby,
        ABS(CAST((CAST(SQRT(id + 1999999777) AS DECIMAL(38, 18)) - CAST(FLOOR(CAST(SQRT(id + 1999999777) AS DECIMAL(38, 18))) AS DECIMAL(38,18))) * 1000000000 AS INT)) % 10000,
        RPAD('Value ' || id || ' ' , 100, '*') as padding
FROM t1;


CREATE TABLE "P_ROW_1000" (
                              a int,
                              b int,
                              c int,
                              padding char(200)
);

INSERT INTO "P_ROW_1000"
    WITH t1 AS
    (
SELECT id
FROM generate_series(0, 999999) id
    )
SELECT  id,
        id % 1000 groupby,
        ABS(CAST((CAST(SQRT(id + 1999999777) AS DECIMAL(38, 18)) - CAST(FLOOR(CAST(SQRT(id + 1999999777) AS DECIMAL(38, 18))) AS DECIMAL(38,18))) * 1000000000 AS INT)) % 10000,
        RPAD('Value ' || id || ' ' , 100, '*') as padding
FROM t1;


CREATE TABLE "P_ROW_3000" (
                              a int,
                              b int,
                              c int,
                              padding char(200)
);

INSERT INTO "P_ROW_3000"
    WITH t1 AS
    (
SELECT id
FROM generate_series(0, 999999) id
    )
SELECT  id,
        id % 3000 groupby,
        ABS(CAST((CAST(SQRT(id + 1999999777) AS DECIMAL(38, 18)) - CAST(FLOOR(CAST(SQRT(id + 1999999777) AS DECIMAL(38, 18))) AS DECIMAL(38,18))) * 1000000000 AS INT)) % 10000,
        RPAD('Value ' || id || ' ' , 100, '*') as padding
FROM t1;


CREATE TABLE "P_ROW_10000" (
                               a int,
                               b int,
                               c int,
                               padding char(200)
);

INSERT INTO "P_ROW_10000"
    WITH t1 AS
    (
SELECT id
FROM generate_series(0, 999999) id
    )
SELECT  id,
        id % 10000 groupby,
        ABS(CAST((CAST(SQRT(id + 1999999777) AS DECIMAL(38, 18)) - CAST(FLOOR(CAST(SQRT(id + 1999999777) AS DECIMAL(38, 18))) AS DECIMAL(38,18))) * 1000000000 AS INT)) % 10000,
        RPAD('Value ' || id || ' ' , 100, '*') as padding
FROM t1;


CREATE TABLE "P_ROW_30000" (
                               a int,
                               b int,
                               c int,
                               padding char(200)
);

INSERT INTO "P_ROW_30000"
    WITH t1 AS
    (
SELECT id
FROM generate_series(0, 999999) id
    )
SELECT  id,
        id % 30000 groupby,
        ABS(CAST((CAST(SQRT(id + 1999999777) AS DECIMAL(38, 18)) - CAST(FLOOR(CAST(SQRT(id + 1999999777) AS DECIMAL(38, 18))) AS DECIMAL(38,18))) * 1000000000 AS INT)) % 10000,
        RPAD('Value ' || id || ' ' , 100, '*') as padding
FROM t1;

