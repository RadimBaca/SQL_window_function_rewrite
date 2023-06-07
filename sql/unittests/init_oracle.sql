-- "Oracle Database 19c Enterprise Edition Release 19.0.0.0.0 - Production Version 19.3.0.0.0"

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE A';
            EXCEPTION
            WHEN OTHERS THEN
    IF SQLCODE != -942 THEN
         RAISE;
END IF;
END;






--Create table A (id int, groupby int, orderby int, fkb int, search int, padding varchar())
CREATE  TABLE A AS
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
             SELECT ones.n + 10 * tens.n + 100 * hundreds.n + 1000 * thousands.n + 10000 * tenthousands.n as id
             FROM x ones,     x tens,      x hundreds,       x thousands,       x tenthousands
         ), t2 AS
         (
             SELECT  id,
                     mod(id, 100) groupby
             FROM t1
         ), t3 AS
         (
             SELECT  b.id, b.groupby, row_number() over (partition by groupby order by id) orderby
             FROM t2 b
         )
SELECT  cast(id as int) id,
        cast(groupby as int) groupby,
        cast(orderby as int) orderby,
        cast(mod(orderby, 9173) as int) fkb,
        cast(mod(id, 911) as int) search,
        RPAD(concat('Value ', id), 1000, '*') as padding
FROM t3;



--insert of some outlier data
insert into A(id, groupby, orderby, fkb, search, padding) values (1000000, null, null, null, null, '*');


insert into A(id, groupby, orderby, fkb, search, padding) values (1000001, 100, null, null, null, '*');


insert into A(id, groupby, orderby, fkb, search, padding) values (1000002, 99, 10000, null, null, '*');


insert into A(id, groupby, orderby, fkb, search, padding) values (1000003, 101, 10000, 1, null, '*');


insert into A(id, groupby, orderby, fkb, search, padding) values (1000004, null, null, 1, null, '*');


insert into A(id, groupby, orderby, fkb, search, padding) values (1000005, null, null, 500, null, '*');


insert into A(id, groupby, orderby, fkb, search, padding) values (1000006, null, null, 1, 1, '*');


insert into A(id, groupby, orderby, fkb, search, padding) values (1000007, null, null, null, 1, '*');





--Set id of both tables as a primary key
alter table A modify id int not null;

alter table A add constraint pk_a_id primary key (id);

