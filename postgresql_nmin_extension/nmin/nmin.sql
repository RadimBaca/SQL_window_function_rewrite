-- 1.0.0
-- NMIN aggreagte function
--

DROP AGGREGATE IF EXISTS  nmin(INT, INT);
DROP FUNCTION IF EXISTS add_numberN(INTEGER[], INT, INT);
DROP FUNCTION IF EXISTS add_number_final(INTEGER[]);

CREATE FUNCTION add_numberN(INTEGER[], INT, INT) RETURNS INTEGER[]
  AS 'nmin', 'add_numberN'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;


CREATE FUNCTION add_number_final(INTEGER[]) RETURNS INTEGER
AS 'nmin', 'add_number_final'
    LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE AGGREGATE nmin(INT, INT)
    (
    INITCOND = '{2147483647}',
    STYPE = INTEGER[],
    SFUNC = add_numberN,
    FINALFUNC = add_number_final
    );