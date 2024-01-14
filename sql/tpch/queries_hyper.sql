-- Q2
select p_partkey, p_mfgr, p_size, p_type, s_acctbal, s_name, n_name, s_address, s_phone, s_comment, rn
from PART
         join (
    select s_acctbal, s_name, n_name, s_address, s_phone, s_comment, ps_partkey,
           row_number() over (partition by ps_partkey order by ps_supplycost) rn
    from PARTSUPP, SUPPLIER, NATION, REGION
    where
            s_suppkey = ps_suppkey and
            s_nationkey = n_nationkey and
            n_regionkey = r_regionkey and
            r_name = 'EUROPE'
) t on PART.p_partkey = t.ps_partkey
where rn = 1 and p_size = 15 and
        p_type like '%BRASS'
order by s_acctbal desc, n_name, s_name, p_partkey
    limit 100;


-- Q15
select s_suppkey, s_name, s_address, s_phone, total_revenue
from
    (
        select s_suppkey, s_name, s_address, s_phone, total_revenue ,
               row_number() over (order by total_revenue desc) rn
        from (
                 select l_suppkey supplier_no, sum(l_extendedprice * (1 - l_discount)) total_revenue
                 from LINEITEM
                 where l_shipdate > '1998-01-01' and l_shipdate < '1998-04-01'
                 group by l_suppkey
        )revenue15, SUPPLIER
        where s_suppkey = supplier_no
    ) t
where rn = 1
order by s_suppkey;


-- Q17
select sum(l_extendedprice) / 7.0 as avg_yearly
from part
join (
    select l_extendedprice, l_quantity, l_partkey,
        avg(l_quantity) over (partition by l_partkey) avg_l_quantity
    from lineitem
) t on l_quantity < 0.2 * avg_l_quantity and p_partkey = l_partkey
where p_brand = 'Brand#23' and
        p_container = 'MED BOX';