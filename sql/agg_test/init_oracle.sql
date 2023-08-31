-- Author : Radim Baca
-- Institution: VSB-Technical University of Ostrava
-- Database system tested: Oracle 19c
-- DDL script that prepares a Oracle database for the microbenchmarks


---------------------------------------------
--------------  Row store -------------------
---------------------------------------------

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE R_row_10';
            EXCEPTION
            WHEN OTHERS THEN
    IF SQLCODE != -942 THEN
         RAISE;
END IF;
END;

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE R_row_30';
            EXCEPTION
            WHEN OTHERS THEN
    IF SQLCODE != -942 THEN
         RAISE;
END IF;
END;

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE R_row_100';
            EXCEPTION
            WHEN OTHERS THEN
    IF SQLCODE != -942 THEN
         RAISE;
END IF;
END;

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE R_row_300';
            EXCEPTION
            WHEN OTHERS THEN
    IF SQLCODE != -942 THEN
         RAISE;
END IF;
END;

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE R_row_1000';
            EXCEPTION
            WHEN OTHERS THEN
    IF SQLCODE != -942 THEN
         RAISE;
END IF;
END;

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE R_row_3000';
            EXCEPTION
            WHEN OTHERS THEN
    IF SQLCODE != -942 THEN
         RAISE;
END IF;
END;

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE R_row_10000';
            EXCEPTION
            WHEN OTHERS THEN
    IF SQLCODE != -942 THEN
         RAISE;
END IF;
END;

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE R_row_30000';
            EXCEPTION
            WHEN OTHERS THEN
    IF SQLCODE != -942 THEN
         RAISE;
END IF;
END;

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE R_row_100000';
            EXCEPTION
            WHEN OTHERS THEN
    IF SQLCODE != -942 THEN
         RAISE;
END IF;
END;

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE R_row_300000';
            EXCEPTION
            WHEN OTHERS THEN
    IF SQLCODE != -942 THEN
         RAISE;
END IF;
END;



CREATE  TABLE R_row_10 AS
WITH x AS
         (
             SELECT 0 n FROM dual
             union all
             SELECT 1 FROM dual
             union all
             SELECT 2 FROM dual
             union all
             SELECT 3 FROM dual
             union all
             SELECT 4 FROM dual
             union all
             SELECT 5 FROM dual
             union all
             SELECT 6 FROM dual
             union all
             SELECT 7 FROM dual
             union all
             SELECT 8 FROM dual
             union all
             SELECT 9 FROM dual
         ), t1 AS
         (
             SELECT ones.n + 10 * tens.n + 100 * hundreds.n + 1000 * thousands.n + 10000 * tenthousands.n + 100000 * hundredthousands.n as id
             FROM x ones,     x tens,      x hundreds,       x thousands,       x tenthousands,       x hundredthousands
         )
SELECT  id A,
        mod(id, 10) B,
        mod(ABS(CAST((CAST(SQRT(id + 1999999777) AS DECIMAL(38, 18)) - CAST(FLOOR(CAST(SQRT(id + 1999999777) AS DECIMAL(38, 18))) AS DECIMAL(38,18))) * 1000000000 AS INT)), 10000) AS C
FROM t1;


CREATE  TABLE R_row_30 AS
WITH x AS
         (
             SELECT 0 n FROM dual
             union all
             SELECT 1 FROM dual
             union all
             SELECT 2 FROM dual
             union all
             SELECT 3 FROM dual
             union all
             SELECT 4 FROM dual
             union all
             SELECT 5 FROM dual
             union all
             SELECT 6 FROM dual
             union all
             SELECT 7 FROM dual
             union all
             SELECT 8 FROM dual
             union all
             SELECT 9 FROM dual
         ), t1 AS
         (
             SELECT ones.n + 10 * tens.n + 100 * hundreds.n + 1000 * thousands.n + 10000 * tenthousands.n + 100000 * hundredthousands.n as id
             FROM x ones,     x tens,      x hundreds,       x thousands,       x tenthousands,       x hundredthousands
         )
SELECT  id A,
        mod(id, 30) B,
        mod(ABS(CAST((CAST(SQRT(id + 1999999777) AS DECIMAL(38, 18)) - CAST(FLOOR(CAST(SQRT(id + 1999999777) AS DECIMAL(38, 18))) AS DECIMAL(38,18))) * 1000000000 AS INT)), 10000) AS C
FROM t1;


CREATE  TABLE R_row_100 AS
WITH x AS
         (
             SELECT 0 n FROM dual
             union all
             SELECT 1 FROM dual
             union all
             SELECT 2 FROM dual
             union all
             SELECT 3 FROM dual
             union all
             SELECT 4 FROM dual
             union all
             SELECT 5 FROM dual
             union all
             SELECT 6 FROM dual
             union all
             SELECT 7 FROM dual
             union all
             SELECT 8 FROM dual
             union all
             SELECT 9 FROM dual
         ), t1 AS
         (
             SELECT ones.n + 10 * tens.n + 100 * hundreds.n + 1000 * thousands.n + 10000 * tenthousands.n + 100000 * hundredthousands.n as id
             FROM x ones,     x tens,      x hundreds,       x thousands,       x tenthousands,       x hundredthousands
         )
SELECT  id A,
        mod(id, 100) B,
        mod(ABS(CAST((CAST(SQRT(id + 1999999777) AS DECIMAL(38, 18)) - CAST(FLOOR(CAST(SQRT(id + 1999999777) AS DECIMAL(38, 18))) AS DECIMAL(38,18))) * 1000000000 AS INT)), 10000) AS C
FROM t1;


CREATE  TABLE R_row_300 AS
WITH x AS
         (
             SELECT 0 n FROM dual
             union all
             SELECT 1 FROM dual
             union all
             SELECT 2 FROM dual
             union all
             SELECT 3 FROM dual
             union all
             SELECT 4 FROM dual
             union all
             SELECT 5 FROM dual
             union all
             SELECT 6 FROM dual
             union all
             SELECT 7 FROM dual
             union all
             SELECT 8 FROM dual
             union all
             SELECT 9 FROM dual
         ), t1 AS
         (
             SELECT ones.n + 10 * tens.n + 100 * hundreds.n + 1000 * thousands.n + 10000 * tenthousands.n + 100000 * hundredthousands.n as id
             FROM x ones,     x tens,      x hundreds,       x thousands,       x tenthousands,       x hundredthousands
         )
SELECT  id A,
        mod(id, 300) B,
        mod(ABS(CAST((CAST(SQRT(id + 1999999777) AS DECIMAL(38, 18)) - CAST(FLOOR(CAST(SQRT(id + 1999999777) AS DECIMAL(38, 18))) AS DECIMAL(38,18))) * 1000000000 AS INT)), 10000) AS C
FROM t1;


CREATE  TABLE R_row_1000 AS
WITH x AS
         (
             SELECT 0 n FROM dual
             union all
             SELECT 1 FROM dual
             union all
             SELECT 2 FROM dual
             union all
             SELECT 3 FROM dual
             union all
             SELECT 4 FROM dual
             union all
             SELECT 5 FROM dual
             union all
             SELECT 6 FROM dual
             union all
             SELECT 7 FROM dual
             union all
             SELECT 8 FROM dual
             union all
             SELECT 9 FROM dual
         ), t1 AS
         (
             SELECT ones.n + 10 * tens.n + 100 * hundreds.n + 1000 * thousands.n + 10000 * tenthousands.n + 100000 * hundredthousands.n as id
             FROM x ones,     x tens,      x hundreds,       x thousands,       x tenthousands,       x hundredthousands
         )
SELECT  id A,
        mod(id, 1000) B,
        mod(ABS(CAST((CAST(SQRT(id + 1999999777) AS DECIMAL(38, 18)) - CAST(FLOOR(CAST(SQRT(id + 1999999777) AS DECIMAL(38, 18))) AS DECIMAL(38,18))) * 1000000000 AS INT)), 10000) AS C
FROM t1;


CREATE  TABLE R_row_3000 AS
WITH x AS
         (
             SELECT 0 n FROM dual
             union all
             SELECT 1 FROM dual
             union all
             SELECT 2 FROM dual
             union all
             SELECT 3 FROM dual
             union all
             SELECT 4 FROM dual
             union all
             SELECT 5 FROM dual
             union all
             SELECT 6 FROM dual
             union all
             SELECT 7 FROM dual
             union all
             SELECT 8 FROM dual
             union all
             SELECT 9 FROM dual
         ), t1 AS
         (
             SELECT ones.n + 10 * tens.n + 100 * hundreds.n + 1000 * thousands.n + 10000 * tenthousands.n + 100000 * hundredthousands.n as id
             FROM x ones,     x tens,      x hundreds,       x thousands,       x tenthousands,       x hundredthousands
         )
SELECT  id A,
        mod(id, 3000) B,
        mod(ABS(CAST((CAST(SQRT(id + 1999999777) AS DECIMAL(38, 18)) - CAST(FLOOR(CAST(SQRT(id + 1999999777) AS DECIMAL(38, 18))) AS DECIMAL(38,18))) * 1000000000 AS INT)), 10000) AS C
FROM t1;


CREATE  TABLE R_row_10000 AS
WITH x AS
         (
             SELECT 0 n FROM dual
             union all
             SELECT 1 FROM dual
             union all
             SELECT 2 FROM dual
             union all
             SELECT 3 FROM dual
             union all
             SELECT 4 FROM dual
             union all
             SELECT 5 FROM dual
             union all
             SELECT 6 FROM dual
             union all
             SELECT 7 FROM dual
             union all
             SELECT 8 FROM dual
             union all
             SELECT 9 FROM dual
         ), t1 AS
         (
             SELECT ones.n + 10 * tens.n + 100 * hundreds.n + 1000 * thousands.n + 10000 * tenthousands.n + 100000 * hundredthousands.n as id
             FROM x ones,     x tens,      x hundreds,       x thousands,       x tenthousands,       x hundredthousands
         )
SELECT  id A,
        mod(id, 10000) B,
        mod(ABS(CAST((CAST(SQRT(id + 1999999777) AS DECIMAL(38, 18)) - CAST(FLOOR(CAST(SQRT(id + 1999999777) AS DECIMAL(38, 18))) AS DECIMAL(38,18))) * 1000000000 AS INT)), 10000) AS C
FROM t1;


CREATE  TABLE R_row_30000 AS
WITH x AS
         (
             SELECT 0 n FROM dual
             union all
             SELECT 1 FROM dual
             union all
             SELECT 2 FROM dual
             union all
             SELECT 3 FROM dual
             union all
             SELECT 4 FROM dual
             union all
             SELECT 5 FROM dual
             union all
             SELECT 6 FROM dual
             union all
             SELECT 7 FROM dual
             union all
             SELECT 8 FROM dual
             union all
             SELECT 9 FROM dual
         ), t1 AS
         (
             SELECT ones.n + 10 * tens.n + 100 * hundreds.n + 1000 * thousands.n + 10000 * tenthousands.n + 100000 * hundredthousands.n as id
             FROM x ones,     x tens,      x hundreds,       x thousands,       x tenthousands,       x hundredthousands
         )
SELECT  id A,
        mod(id, 30000) B,
        mod(ABS(CAST((CAST(SQRT(id + 1999999777) AS DECIMAL(38, 18)) - CAST(FLOOR(CAST(SQRT(id + 1999999777) AS DECIMAL(38, 18))) AS DECIMAL(38,18))) * 1000000000 AS INT)), 10000) AS C
FROM t1;


CREATE  TABLE R_row_100000 AS
WITH x AS
         (
             SELECT 0 n FROM dual
             union all
             SELECT 1 FROM dual
             union all
             SELECT 2 FROM dual
             union all
             SELECT 3 FROM dual
             union all
             SELECT 4 FROM dual
             union all
             SELECT 5 FROM dual
             union all
             SELECT 6 FROM dual
             union all
             SELECT 7 FROM dual
             union all
             SELECT 8 FROM dual
             union all
             SELECT 9 FROM dual
         ), t1 AS
         (
             SELECT ones.n + 10 * tens.n + 100 * hundreds.n + 1000 * thousands.n + 10000 * tenthousands.n + 100000 * hundredthousands.n as id
             FROM x ones,     x tens,      x hundreds,       x thousands,       x tenthousands,       x hundredthousands
         )
SELECT  id A,
        mod(id, 100000) B,
        mod(ABS(CAST((CAST(SQRT(id + 1999999777) AS DECIMAL(38, 18)) - CAST(FLOOR(CAST(SQRT(id + 1999999777) AS DECIMAL(38, 18))) AS DECIMAL(38,18))) * 1000000000 AS INT)), 10000) AS C
FROM t1;


CREATE  TABLE R_row_300000 AS
WITH x AS
         (
             SELECT 0 n FROM dual
             union all
             SELECT 1 FROM dual
             union all
             SELECT 2 FROM dual
             union all
             SELECT 3 FROM dual
             union all
             SELECT 4 FROM dual
             union all
             SELECT 5 FROM dual
             union all
             SELECT 6 FROM dual
             union all
             SELECT 7 FROM dual
             union all
             SELECT 8 FROM dual
             union all
             SELECT 9 FROM dual
         ), t1 AS
         (
             SELECT ones.n + 10 * tens.n + 100 * hundreds.n + 1000 * thousands.n + 10000 * tenthousands.n + 100000 * hundredthousands.n as id
             FROM x ones,     x tens,      x hundreds,       x thousands,       x tenthousands,       x hundredthousands
         )
SELECT  id A,
        mod(id, 300000) B,
        mod(ABS(CAST((CAST(SQRT(id + 1999999777) AS DECIMAL(38, 18)) - CAST(FLOOR(CAST(SQRT(id + 1999999777) AS DECIMAL(38, 18))) AS DECIMAL(38,18))) * 1000000000 AS INT)), 10000) AS C
FROM t1;






-------------------------------------------------------------------------
-- Tables with padding



BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE P_row_10';
            EXCEPTION
            WHEN OTHERS THEN
    IF SQLCODE != -942 THEN
         RAISE;
END IF;
END;

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE P_row_30';
            EXCEPTION
            WHEN OTHERS THEN
    IF SQLCODE != -942 THEN
         RAISE;
END IF;
END;

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE P_row_100';
            EXCEPTION
            WHEN OTHERS THEN
    IF SQLCODE != -942 THEN
         RAISE;
END IF;
END;

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE P_row_300';
            EXCEPTION
            WHEN OTHERS THEN
    IF SQLCODE != -942 THEN
         RAISE;
END IF;
END;

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE P_row_1000';
            EXCEPTION
            WHEN OTHERS THEN
    IF SQLCODE != -942 THEN
         RAISE;
END IF;
END;

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE P_row_3000';
            EXCEPTION
            WHEN OTHERS THEN
    IF SQLCODE != -942 THEN
         RAISE;
END IF;
END;

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE P_row_10000';
            EXCEPTION
            WHEN OTHERS THEN
    IF SQLCODE != -942 THEN
         RAISE;
END IF;
END;

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE P_row_30000';
            EXCEPTION
            WHEN OTHERS THEN
    IF SQLCODE != -942 THEN
         RAISE;
END IF;
END;

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE P_row_100000';
            EXCEPTION
            WHEN OTHERS THEN
    IF SQLCODE != -942 THEN
         RAISE;
END IF;
END;

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE P_row_300000';
            EXCEPTION
            WHEN OTHERS THEN
    IF SQLCODE != -942 THEN
         RAISE;
END IF;
END;



CREATE  TABLE P_row_10 AS
WITH x AS
         (
             SELECT 0 n FROM dual
             union all
             SELECT 1 FROM dual
             union all
             SELECT 2 FROM dual
             union all
             SELECT 3 FROM dual
             union all
             SELECT 4 FROM dual
             union all
             SELECT 5 FROM dual
             union all
             SELECT 6 FROM dual
             union all
             SELECT 7 FROM dual
             union all
             SELECT 8 FROM dual
             union all
             SELECT 9 FROM dual
         ), t1 AS
         (
             SELECT ones.n + 10 * tens.n + 100 * hundreds.n + 1000 * thousands.n + 10000 * tenthousands.n + 100000 * hundredthousands.n as id
             FROM x ones,     x tens,      x hundreds,       x thousands,       x tenthousands,       x hundredthousands
         )
SELECT  id A,
        mod(id, 10) B,
        mod(ABS(CAST((CAST(SQRT(id + 1999999777) AS DECIMAL(38, 18)) - CAST(FLOOR(CAST(SQRT(id + 1999999777) AS DECIMAL(38, 18))) AS DECIMAL(38,18))) * 1000000000 AS INT)), 10000) AS C,
        RPAD(concat('Value ', id), 100, '*') as padding
FROM t1;


CREATE  TABLE P_row_30 AS
WITH x AS
         (
             SELECT 0 n FROM dual
             union all
             SELECT 1 FROM dual
             union all
             SELECT 2 FROM dual
             union all
             SELECT 3 FROM dual
             union all
             SELECT 4 FROM dual
             union all
             SELECT 5 FROM dual
             union all
             SELECT 6 FROM dual
             union all
             SELECT 7 FROM dual
             union all
             SELECT 8 FROM dual
             union all
             SELECT 9 FROM dual
         ), t1 AS
         (
             SELECT ones.n + 10 * tens.n + 100 * hundreds.n + 1000 * thousands.n + 10000 * tenthousands.n + 100000 * hundredthousands.n as id
             FROM x ones,     x tens,      x hundreds,       x thousands,       x tenthousands,       x hundredthousands
         )
SELECT  id A,
        mod(id, 30) B,
        mod(ABS(CAST((CAST(SQRT(id + 1999999777) AS DECIMAL(38, 18)) - CAST(FLOOR(CAST(SQRT(id + 1999999777) AS DECIMAL(38, 18))) AS DECIMAL(38,18))) * 1000000000 AS INT)), 10000) AS C,
        RPAD(concat('Value ', id), 100, '*') as padding
FROM t1;


CREATE  TABLE P_row_100 AS
WITH x AS
         (
             SELECT 0 n FROM dual
             union all
             SELECT 1 FROM dual
             union all
             SELECT 2 FROM dual
             union all
             SELECT 3 FROM dual
             union all
             SELECT 4 FROM dual
             union all
             SELECT 5 FROM dual
             union all
             SELECT 6 FROM dual
             union all
             SELECT 7 FROM dual
             union all
             SELECT 8 FROM dual
             union all
             SELECT 9 FROM dual
         ), t1 AS
         (
             SELECT ones.n + 10 * tens.n + 100 * hundreds.n + 1000 * thousands.n + 10000 * tenthousands.n + 100000 * hundredthousands.n as id
             FROM x ones,     x tens,      x hundreds,       x thousands,       x tenthousands,       x hundredthousands
         )
SELECT  id A,
        mod(id, 100) B,
        mod(ABS(CAST((CAST(SQRT(id + 1999999777) AS DECIMAL(38, 18)) - CAST(FLOOR(CAST(SQRT(id + 1999999777) AS DECIMAL(38, 18))) AS DECIMAL(38,18))) * 1000000000 AS INT)), 10000) AS C,
        RPAD(concat('Value ', id), 100, '*') as padding
FROM t1;


CREATE  TABLE P_row_300 AS
WITH x AS
         (
             SELECT 0 n FROM dual
             union all
             SELECT 1 FROM dual
             union all
             SELECT 2 FROM dual
             union all
             SELECT 3 FROM dual
             union all
             SELECT 4 FROM dual
             union all
             SELECT 5 FROM dual
             union all
             SELECT 6 FROM dual
             union all
             SELECT 7 FROM dual
             union all
             SELECT 8 FROM dual
             union all
             SELECT 9 FROM dual
         ), t1 AS
         (
             SELECT ones.n + 10 * tens.n + 100 * hundreds.n + 1000 * thousands.n + 10000 * tenthousands.n + 100000 * hundredthousands.n as id
             FROM x ones,     x tens,      x hundreds,       x thousands,       x tenthousands,       x hundredthousands
         )
SELECT  id A,
        mod(id, 300) B,
        mod(ABS(CAST((CAST(SQRT(id + 1999999777) AS DECIMAL(38, 18)) - CAST(FLOOR(CAST(SQRT(id + 1999999777) AS DECIMAL(38, 18))) AS DECIMAL(38,18))) * 1000000000 AS INT)), 10000) AS C,
        RPAD(concat('Value ', id), 100, '*') as padding
FROM t1;


CREATE  TABLE P_row_1000 AS
WITH x AS
         (
             SELECT 0 n FROM dual
             union all
             SELECT 1 FROM dual
             union all
             SELECT 2 FROM dual
             union all
             SELECT 3 FROM dual
             union all
             SELECT 4 FROM dual
             union all
             SELECT 5 FROM dual
             union all
             SELECT 6 FROM dual
             union all
             SELECT 7 FROM dual
             union all
             SELECT 8 FROM dual
             union all
             SELECT 9 FROM dual
         ), t1 AS
         (
             SELECT ones.n + 10 * tens.n + 100 * hundreds.n + 1000 * thousands.n + 10000 * tenthousands.n + 100000 * hundredthousands.n as id
             FROM x ones,     x tens,      x hundreds,       x thousands,       x tenthousands,       x hundredthousands
         )
SELECT  id A,
        mod(id, 1000) B,
        mod(ABS(CAST((CAST(SQRT(id + 1999999777) AS DECIMAL(38, 18)) - CAST(FLOOR(CAST(SQRT(id + 1999999777) AS DECIMAL(38, 18))) AS DECIMAL(38,18))) * 1000000000 AS INT)), 10000) AS C,
        RPAD(concat('Value ', id), 100, '*') as padding
FROM t1;


CREATE  TABLE P_row_3000 AS
WITH x AS
         (
             SELECT 0 n FROM dual
             union all
             SELECT 1 FROM dual
             union all
             SELECT 2 FROM dual
             union all
             SELECT 3 FROM dual
             union all
             SELECT 4 FROM dual
             union all
             SELECT 5 FROM dual
             union all
             SELECT 6 FROM dual
             union all
             SELECT 7 FROM dual
             union all
             SELECT 8 FROM dual
             union all
             SELECT 9 FROM dual
         ), t1 AS
         (
             SELECT ones.n + 10 * tens.n + 100 * hundreds.n + 1000 * thousands.n + 10000 * tenthousands.n + 100000 * hundredthousands.n as id
             FROM x ones,     x tens,      x hundreds,       x thousands,       x tenthousands,       x hundredthousands
         )
SELECT  id A,
        mod(id, 3000) B,
        mod(ABS(CAST((CAST(SQRT(id + 1999999777) AS DECIMAL(38, 18)) - CAST(FLOOR(CAST(SQRT(id + 1999999777) AS DECIMAL(38, 18))) AS DECIMAL(38,18))) * 1000000000 AS INT)), 10000) AS C,
        RPAD(concat('Value ', id), 100, '*') as padding
FROM t1;


CREATE  TABLE P_row_10000 AS
WITH x AS
         (
             SELECT 0 n FROM dual
             union all
             SELECT 1 FROM dual
             union all
             SELECT 2 FROM dual
             union all
             SELECT 3 FROM dual
             union all
             SELECT 4 FROM dual
             union all
             SELECT 5 FROM dual
             union all
             SELECT 6 FROM dual
             union all
             SELECT 7 FROM dual
             union all
             SELECT 8 FROM dual
             union all
             SELECT 9 FROM dual
         ), t1 AS
         (
             SELECT ones.n + 10 * tens.n + 100 * hundreds.n + 1000 * thousands.n + 10000 * tenthousands.n + 100000 * hundredthousands.n as id
             FROM x ones,     x tens,      x hundreds,       x thousands,       x tenthousands,       x hundredthousands
         )
SELECT  id A,
        mod(id, 10000) B,
        mod(ABS(CAST((CAST(SQRT(id + 1999999777) AS DECIMAL(38, 18)) - CAST(FLOOR(CAST(SQRT(id + 1999999777) AS DECIMAL(38, 18))) AS DECIMAL(38,18))) * 1000000000 AS INT)), 10000) AS C,
        RPAD(concat('Value ', id), 100, '*') as padding
FROM t1;


CREATE  TABLE P_row_30000 AS
WITH x AS
         (
             SELECT 0 n FROM dual
             union all
             SELECT 1 FROM dual
             union all
             SELECT 2 FROM dual
             union all
             SELECT 3 FROM dual
             union all
             SELECT 4 FROM dual
             union all
             SELECT 5 FROM dual
             union all
             SELECT 6 FROM dual
             union all
             SELECT 7 FROM dual
             union all
             SELECT 8 FROM dual
             union all
             SELECT 9 FROM dual
         ), t1 AS
         (
             SELECT ones.n + 10 * tens.n + 100 * hundreds.n + 1000 * thousands.n + 10000 * tenthousands.n + 100000 * hundredthousands.n as id
             FROM x ones,     x tens,      x hundreds,       x thousands,       x tenthousands,       x hundredthousands
         )
SELECT  id A,
        mod(id, 30000) B,
        mod(ABS(CAST((CAST(SQRT(id + 1999999777) AS DECIMAL(38, 18)) - CAST(FLOOR(CAST(SQRT(id + 1999999777) AS DECIMAL(38, 18))) AS DECIMAL(38,18))) * 1000000000 AS INT)), 10000) AS C,
        RPAD(concat('Value ', id), 100, '*') as padding
FROM t1;


CREATE  TABLE P_row_100000 AS
WITH x AS
         (
             SELECT 0 n FROM dual
             union all
             SELECT 1 FROM dual
             union all
             SELECT 2 FROM dual
             union all
             SELECT 3 FROM dual
             union all
             SELECT 4 FROM dual
             union all
             SELECT 5 FROM dual
             union all
             SELECT 6 FROM dual
             union all
             SELECT 7 FROM dual
             union all
             SELECT 8 FROM dual
             union all
             SELECT 9 FROM dual
         ), t1 AS
         (
             SELECT ones.n + 10 * tens.n + 100 * hundreds.n + 1000 * thousands.n + 10000 * tenthousands.n + 100000 * hundredthousands.n as id
             FROM x ones,     x tens,      x hundreds,       x thousands,       x tenthousands,       x hundredthousands
         )
SELECT  id A,
        mod(id, 100000) B,
        mod(ABS(CAST((CAST(SQRT(id + 1999999777) AS DECIMAL(38, 18)) - CAST(FLOOR(CAST(SQRT(id + 1999999777) AS DECIMAL(38, 18))) AS DECIMAL(38,18))) * 1000000000 AS INT)), 10000) AS C,
        RPAD(concat('Value ', id), 100, '*') as padding
FROM t1;


CREATE  TABLE P_row_300000 AS
WITH x AS
         (
             SELECT 0 n FROM dual
             union all
             SELECT 1 FROM dual
             union all
             SELECT 2 FROM dual
             union all
             SELECT 3 FROM dual
             union all
             SELECT 4 FROM dual
             union all
             SELECT 5 FROM dual
             union all
             SELECT 6 FROM dual
             union all
             SELECT 7 FROM dual
             union all
             SELECT 8 FROM dual
             union all
             SELECT 9 FROM dual
         ), t1 AS
         (
             SELECT ones.n + 10 * tens.n + 100 * hundreds.n + 1000 * thousands.n + 10000 * tenthousands.n + 100000 * hundredthousands.n as id
             FROM x ones,     x tens,      x hundreds,       x thousands,       x tenthousands,       x hundredthousands
         )
SELECT  id A,
        mod(id, 300000) B,
        mod(ABS(CAST((CAST(SQRT(id + 1999999777) AS DECIMAL(38, 18)) - CAST(FLOOR(CAST(SQRT(id + 1999999777) AS DECIMAL(38, 18))) AS DECIMAL(38,18))) * 1000000000 AS INT)), 10000) AS C,
        RPAD(concat('Value ', id), 100, '*') as padding
FROM t1;



---------------------------------------------
-------------  Column store -----------------
---------------------------------------------


-------------------------------------------------------------------------
-- inmemory column store setting
-- this needs
-- alter system set inmemory_size=2G scope=spfile;

create table R_column_10 as
select * from R_ROW_10;
alter table R_column_10 inmemory;
select /*+ FULL(R_column_10) NO_PARALLEL(R_column_10) */ * from R_column_10;

create table R_column_30 as
select * from R_ROW_30;
alter table R_column_30 inmemory;
select /*+ FULL(R_column_30) NO_PARALLEL(R_column_30) */ * from R_column_30;

create table R_column_100 as
select * from R_ROW_100;
alter table R_column_100 inmemory;
select /*+ FULL(R_column_100) NO_PARALLEL(R_column_100) */ * from R_column_100;

create table R_column_300 as
select * from R_ROW_300;
alter table R_column_300 inmemory;
select /*+ FULL(R_column_300) NO_PARALLEL(R_column_300) */ * from R_column_300;

create table R_column_1000 as
select * from R_ROW_1000;
alter table R_column_1000 inmemory;
select /*+ FULL(R_column_1000) NO_PARALLEL(R_column_1000) */ * from R_column_1000;

create table R_column_3000 as
select * from R_ROW_3000;
alter table R_column_3000 inmemory;
select /*+ FULL(R_column_3000) NO_PARALLEL(R_column_3000) */ * from R_column_3000;

create table R_column_10000 as
select * from R_ROW_10000;
alter table R_column_10000 inmemory;
select /*+ FULL(R_column_10000) NO_PARALLEL(R_column_10000) */ * from R_column_10000;

create table R_column_30000 as
select * from R_ROW_30000;
alter table R_column_30000 inmemory;
select /*+ FULL(R_column_30000) NO_PARALLEL(R_column_30000) */ * from R_column_30000;

-- Padding tables

create table P_column_10 as
select * from P_ROW_10;
alter table P_column_10 inmemory;
select /*+ FULL(P_column_10) NO_PARALLEL(P_column_10) */ * from P_column_10;

create table P_column_30 as
select * from P_ROW_30;
alter table P_column_30 inmemory;
select /*+ FULL(P_column_30) NO_PARALLEL(P_column_30) */ * from P_column_30;

create table P_column_100 as
select * from P_ROW_100;
alter table P_column_100 inmemory;
select /*+ FULL(P_column_100) NO_PARALLEL(P_column_100) */ * from P_column_100;

create table P_column_300 as
select * from P_ROW_300;
alter table P_column_300 inmemory;
select /*+ FULL(P_column_300) NO_PARALLEL(P_column_300) */ * from P_column_300;

create table P_column_1000 as
select * from P_ROW_1000;
alter table P_column_1000 inmemory;
select /*+ FULL(P_column_1000) NO_PARALLEL(P_column_1000) */ * from P_column_1000;

create table P_column_3000 as
select * from P_ROW_3000;
alter table P_column_3000 inmemory;
select /*+ FULL(P_column_3000) NO_PARALLEL(P_column_3000) */ * from P_column_3000;

create table P_column_10000 as
select * from P_ROW_10000;
alter table P_column_10000 inmemory;
select /*+ FULL(P_column_10000) NO_PARALLEL(P_column_10000) */ * from P_column_10000;

create table P_column_30000 as
select * from P_ROW_30000;
alter table P_column_30000 inmemory;
select /*+ FULL(P_column_30000) NO_PARALLEL(P_column_30000) */ * from P_column_30000;

