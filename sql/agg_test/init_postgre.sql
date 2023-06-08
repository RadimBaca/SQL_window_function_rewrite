DROP TABLE IF EXISTS R_row_10 CASCADE;
DROP TABLE IF EXISTS R_row_30 CASCADE;
DROP TABLE IF EXISTS R_row_100 CASCADE;
DROP TABLE IF EXISTS R_row_300 CASCADE;
DROP TABLE IF EXISTS R_row_1000 CASCADE;
DROP TABLE IF EXISTS R_row_3000 CASCADE;
DROP TABLE IF EXISTS R_row_10000 CASCADE;
DROP TABLE IF EXISTS R_row_30000 CASCADE;


CREATE TABLE R_row_10 (
                          a int,
                          b int,
                          c int
);

INSERT INTO R_row_10
    WITH t1 AS
    (
SELECT id
FROM generate_series(0, 999999) id
    )
SELECT  id,
        id % 10 groupby,
        ABS(CAST((CAST(SQRT(id + 1999999777) AS DECIMAL(38, 18)) - CAST(FLOOR(CAST(SQRT(id + 1999999777) AS DECIMAL(38, 18))) AS DECIMAL(38,18))) * 1000000000 AS INT)) % 10000
FROM t1;

CREATE TABLE R_row_30 (
                          a int,
                          b int,
                          c int
);

INSERT INTO R_row_30
    WITH t1 AS
    (
SELECT id
FROM generate_series(0, 999999) id
    )
SELECT  id,
        id % 30 groupby,
        ABS(CAST((CAST(SQRT(id + 1999999777) AS DECIMAL(38, 18)) - CAST(FLOOR(CAST(SQRT(id + 1999999777) AS DECIMAL(38, 18))) AS DECIMAL(38,18))) * 1000000000 AS INT)) % 10000
FROM t1;


CREATE TABLE R_row_100 (
                           a int,
                           b int,
                           c int
);

INSERT INTO R_row_100
    WITH t1 AS
    (
SELECT id
FROM generate_series(0, 999999) id
    )
SELECT  id,
        id % 100 groupby,
        ABS(CAST((CAST(SQRT(id + 1999999777) AS DECIMAL(38, 18)) - CAST(FLOOR(CAST(SQRT(id + 1999999777) AS DECIMAL(38, 18))) AS DECIMAL(38,18))) * 1000000000 AS INT)) % 10000
FROM t1;


CREATE TABLE R_row_300 (
                           a int,
                           b int,
                           c int
);

INSERT INTO R_row_300
    WITH t1 AS
    (
SELECT id
FROM generate_series(0, 999999) id
    )
SELECT  id,
        id % 300 groupby,
        ABS(CAST((CAST(SQRT(id + 1999999777) AS DECIMAL(38, 18)) - CAST(FLOOR(CAST(SQRT(id + 1999999777) AS DECIMAL(38, 18))) AS DECIMAL(38,18))) * 1000000000 AS INT)) % 10000
FROM t1;


CREATE TABLE R_row_1000 (
                            a int,
                            b int,
                            c int
);

INSERT INTO R_row_1000
    WITH t1 AS
    (
SELECT id
FROM generate_series(0, 999999) id
    )
SELECT  id,
        id % 1000 groupby,
        ABS(CAST((CAST(SQRT(id + 1999999777) AS DECIMAL(38, 18)) - CAST(FLOOR(CAST(SQRT(id + 1999999777) AS DECIMAL(38, 18))) AS DECIMAL(38,18))) * 1000000000 AS INT)) % 10000
FROM t1;


CREATE TABLE R_row_3000 (
                            a int,
                            b int,
                            c int
);

INSERT INTO R_row_3000
    WITH t1 AS
    (
SELECT id
FROM generate_series(0, 999999) id
    )
SELECT  id,
        id % 3000 groupby,
        ABS(CAST((CAST(SQRT(id + 1999999777) AS DECIMAL(38, 18)) - CAST(FLOOR(CAST(SQRT(id + 1999999777) AS DECIMAL(38, 18))) AS DECIMAL(38,18))) * 1000000000 AS INT)) % 10000
FROM t1;


CREATE TABLE R_row_10000 (
                             a int,
                             b int,
                             c int
);

INSERT INTO R_row_10000
    WITH t1 AS
    (
SELECT id
FROM generate_series(0, 999999) id
    )
SELECT  id,
        id % 10000 groupby,
        ABS(CAST((CAST(SQRT(id + 1999999777) AS DECIMAL(38, 18)) - CAST(FLOOR(CAST(SQRT(id + 1999999777) AS DECIMAL(38, 18))) AS DECIMAL(38,18))) * 1000000000 AS INT)) % 10000
FROM t1;


CREATE TABLE R_row_30000 (
                             a int,
                             b int,
                             c int
);

INSERT INTO R_row_30000
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

DROP TABLE IF EXISTS P_row_10 CASCADE;
DROP TABLE IF EXISTS P_row_30 CASCADE;
DROP TABLE IF EXISTS P_row_100 CASCADE;
DROP TABLE IF EXISTS P_row_300 CASCADE;
DROP TABLE IF EXISTS P_row_1000 CASCADE;
DROP TABLE IF EXISTS P_row_3000 CASCADE;
DROP TABLE IF EXISTS P_row_10000 CASCADE;
DROP TABLE IF EXISTS P_row_30000 CASCADE;


CREATE TABLE P_row_10 (
                          a int,
                          b int,
                          c int,
                          padding char(200)
);

INSERT INTO P_row_10
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

CREATE TABLE P_row_30 (
                          a int,
                          b int,
                          c int,
                          padding char(200)
);

INSERT INTO P_row_30
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


CREATE TABLE P_row_100 (
                           a int,
                           b int,
                           c int,
                           padding char(200)
);

INSERT INTO P_row_100
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


CREATE TABLE P_row_300 (
                           a int,
                           b int,
                           c int,
                           padding char(200)
);

INSERT INTO P_row_300
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


CREATE TABLE P_row_1000 (
                            a int,
                            b int,
                            c int,
                            padding char(200)
);

INSERT INTO P_row_1000
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


CREATE TABLE P_row_3000 (
                            a int,
                            b int,
                            c int,
                            padding char(200)
);

INSERT INTO P_row_3000
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


CREATE TABLE P_row_10000 (
                             a int,
                             b int,
                             c int,
                             padding char(200)
);

INSERT INTO P_row_10000
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


CREATE TABLE P_row_30000 (
                             a int,
                             b int,
                             c int,
                             padding char(200)
);

INSERT INTO P_row_30000
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

