

with tab as (
    select 1 g, null m
    union all
    select 1, 1
    union all
    select 1, 2
    union all
    select 1, 3
    union all
    select 1, 4
    union all
    select 1, 5
    union all
    select 1, 6
    union all
    select 1, 7
    union all
    select 1, 8
    union all
    select 1, 9
    union all
    select 1, 10
    union all
    select 1, 11
    union all
    select 2, 2
    union all
    select 2, 3
)
select g, nmin(m, 10) from tab group by g;

-- The expected result is
-- g,nmin
-- 1,10
-- 2,2147483647