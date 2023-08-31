-- Author : Radim Baca
-- Institution: VSB-Technical University of Ostrava
-- Database system tested: SQL Server 2017
-- DDL script that prepares a SQL Server database for the unit testing

Drop table if exists A


WITH x AS
         (
             SELECT n FROM (VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) v(n)
         ), t1 AS
         (
             SELECT ones.n + 10 * tens.n + 100 * hundreds.n + 1000 * thousands.n + 10000 * tenthousands.n  as id
             FROM x ones,     x tens,      x hundreds,       x thousands,       x tenthousands
         ), t2 AS
         (
             SELECT  id,
                     id % 100 groupby
             FROM t1
         ), t3 AS
         (
             SELECT  b.id, b.groupby, row_number() over (partition by groupby order by id) orderby
             FROM t2 b
         )
SELECT  cast(id as int) id,
        cast(groupby as int) groupby,
        cast(orderby as int) orderby,
        cast(orderby % 9173 as int) fkb,
        cast (id % 911 as int) search,
        LEFT('Value ' + CAST(id AS VARCHAR) + ' ' + REPLICATE('*', 1000), 1000) as padding
INTO A
FROM t3

GO


--insert of some outlier data
insert into A(id, groupby, orderby, fkb, search, padding) values (1000000, null, null, null, null, '*');
insert into A(id, groupby, orderby, fkb, search, padding) values (1000001, 100, null, null, null, '*');
insert into A(id, groupby, orderby, fkb, search, padding) values (1000002, 99, 10000, null, null, '*');
insert into A(id, groupby, orderby, fkb, search, padding) values (1000003, 101, 10000, 1, null, '*');
insert into A(id, groupby, orderby, fkb, search, padding) values (1000004, null, null, 1, null, '*');
insert into A(id, groupby, orderby, fkb, search, padding) values (1000005, null, null, 500, null, '*');
insert into A(id, groupby, orderby, fkb, search, padding) values (1000006, null, null, 1, 1, '*');
insert into A(id, groupby, orderby, fkb, search, padding) values (1000007, null, null, null, 1, '*');

insert into A(id, groupby, orderby, fkb, search, padding) values (1000008, 1000, null, 1, null, '*');
insert into A(id, groupby, orderby, fkb, search, padding) values (1000009, 1000, null, 2, null, '*');
insert into A(id, groupby, orderby, fkb, search, padding) values (1000010, 1000, 1, 3, null, '*');
insert into A(id, groupby, orderby, fkb, search, padding) values (1000011, 1000, 1, 4, null, '*');
insert into A(id, groupby, orderby, fkb, search, padding) values (1000012, 1000, 1, 5, 1, '*');
insert into A(id, groupby, orderby, fkb, search, padding) values (1000013, 1000, 1, 6, 1, '*');
insert into A(id, groupby, orderby, fkb, search, padding) values (1000014, 1000, 1, 7, 2, '*');
insert into A(id, groupby, orderby, fkb, search, padding) values (1000015, 1000, 1, 8, 2, '*');
insert into A(id, groupby, orderby, fkb, search, padding) values (1000016, 1000, 2, 9, null, '*');
insert into A(id, groupby, orderby, fkb, search, padding) values (1000017, 1000, 2, 10, null, '*');
insert into A(id, groupby, orderby, fkb, search, padding) values (1000018, 1000, 2, 11, 1, '*');
insert into A(id, groupby, orderby, fkb, search, padding) values (1000019, 1000, 2, 12, 1, '*');
insert into A(id, groupby, orderby, fkb, search, padding) values (1000020, 1000, 2, 13, 2, '*');
insert into A(id, groupby, orderby, fkb, search, padding) values (1000021, 1000, 2, 14, 2, '*');

GO

alter table A alter column id int not null;
GO

alter table A add constraint pk_a_id primary key (id);


