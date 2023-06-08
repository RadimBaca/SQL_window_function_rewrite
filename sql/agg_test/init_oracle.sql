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

