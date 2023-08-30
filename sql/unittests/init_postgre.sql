-- PostgreSQL version 14.2.

-- Author : Radim Baca
-- Institution: VSB-Technical University of Ostrava
-- Database system tested: PostgreSQL 14.2
-- DDL script that prepares a PostgreSQL database for the unit testing


DROP TABLE IF EXISTS A CASCADE;


CREATE TABLE A (
                   id int,
                   groupby int,
                   orderby int,
                   fkb int,
                   search int,
                   padding varchar(1000)
);




INSERT
INTO    A
    WITH t1 AS
    (
SELECT id
FROM generate_series(0, 99999) id
    ), t2 AS
(
    SELECT  id,
            id % 100 groupby
    FROM t1
), t3 AS
(
    SELECT  b.id,
            b.groupby,
            row_number() over (partition by groupby order by id) orderby
    FROM t2 b
)
SELECT  id,
        groupby,
        orderby orderby,
        cast(orderby % 9173 as int) fkb,
        cast (id % 911 as int) lsearch,
        RPAD('Value ' || id || ' ' , 1000, '*') as padding
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
alter table A alter column id set not null;


alter table A add constraint pk_a_id primary key (id);
