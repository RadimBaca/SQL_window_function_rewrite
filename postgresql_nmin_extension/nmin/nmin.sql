-- 1.0.0
-- NMIN aggreagte function
-- just a friendly reminder that you need to be a superuser to install this

DROP AGGREGATE IF EXISTS  nmin(INT, INT);
DROP FUNCTION IF EXISTS add_number(INTEGER[], INT, INT);
DROP FUNCTION IF EXISTS add_number_final(INTEGER[]);

CREATE FUNCTION add_number(INTEGER[], INT, INT) RETURNS INTEGER[]
                                                               AS 'nmin', 'add_number'
LANGUAGE C IMMUTABLE PARALLEL SAFE;


CREATE FUNCTION add_number_final(INTEGER[]) RETURNS INTEGER
AS 'nmin', 'add_number_final'
    LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE AGGREGATE nmin(INT, INT)
    (
    INITCOND = '{2147483647}',
    STYPE = INTEGER[],
    SFUNC = add_number,
    FINALFUNC = add_number_final
    );