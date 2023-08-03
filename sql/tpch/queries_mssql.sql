-- PostgreSQL 

-- Q2
-- Original query
        explain analyse
select s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment
from PART, SUPPLIER, PARTSUPP, NATION, REGION
where p_partkey = ps_partkey and s_suppkey = ps_suppkey and
        p_size = 15 and
        p_type like '%BRASS' and
        s_nationkey = n_nationkey and
        n_regionkey = r_regionkey and
        r_name = 'EUROPE' and
        ps_supplycost = (
        select min(ps_supplycost)
        from PARTSUPP, SUPPLIER, NATION, REGION
        where p_partkey = ps_partkey and
                s_suppkey = ps_suppkey and
                s_nationkey = n_nationkey and
                n_regionkey = r_regionkey and
                r_name = 'EUROPE'
    )
order by s_acctbal desc, n_name, s_name, p_partkey
    limit 100;


-- Window function query
        explain analyse
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

-- Produced by the SQL rewriting tool
        explain analyse
SELECT P_PARTKEY,  P_MFGR,  P_SIZE,  P_TYPE,  S_ACCTBAL,  S_NAME,  N_NAME,  S_ADDRESS,  S_PHONE,  S_COMMENT,  RN
FROM
    PART
        JOIN (
        SELECT
            main_subquery.S_ACCTBAL,
            main_subquery.S_NAME,
            main_subquery.N_NAME,
            main_subquery.S_ADDRESS,
            main_subquery.S_PHONE,
            main_subquery.S_COMMENT,
            main_subquery.PS_PARTKEY,
            RN
        FROM
            (
                SELECT S_ACCTBAL, S_NAME,  N_NAME,  S_ADDRESS,  S_PHONE,  S_COMMENT,  PS_PARTKEY,  PS_SUPPLYCOST
                FROM
                    PARTSUPP,
                    SUPPLIER,
                    NATION,
                    REGION
                WHERE
                        S_SUPPKEY = PS_SUPPKEY
                  AND S_NATIONKEY = N_NATIONKEY
                  AND N_REGIONKEY = R_REGIONKEY
                  AND R_NAME = 'EUROPE'
            ) main_subquery
                JOIN (
                SELECT
                    PS_PARTKEY,
                    MIN(PS_SUPPLYCOST) min_PS_SUPPLYCOST,
                    1 RN
                FROM
                    (
                        SELECT S_ACCTBAL, S_NAME, N_NAME, S_ADDRESS, S_PHONE, S_COMMENT, PS_PARTKEY, PS_SUPPLYCOST
                        FROM
                            PARTSUPP,
                            SUPPLIER,
                            NATION,
                            REGION
                        WHERE
                                S_SUPPKEY = PS_SUPPKEY
                          AND S_NATIONKEY = N_NATIONKEY
                          AND N_REGIONKEY = R_REGIONKEY
                          AND R_NAME = 'EUROPE'
                    ) winfun_subquery
                GROUP BY
                    PS_PARTKEY
            ) win_subquery_1 ON (
                                        win_subquery_1.PS_PARTKEY = main_subquery.PS_PARTKEY
                                    )
                AND win_subquery_1.min_PS_SUPPLYCOST = main_subquery.PS_SUPPLYCOST
    ) T ON PART.P_PARTKEY = T.PS_PARTKEY
WHERE
        RN = 1
  AND P_SIZE = 15
  AND P_TYPE LIKE '%BRASS'
ORDER BY
    S_ACCTBAL DESC,
    N_NAME,
    S_NAME,
    P_PARTKEY
    limit 100;







-- q15
create view revenue15 (supplier_no, total_revenue) as
select l_suppkey, sum(l_extendedprice * (1 - l_discount))
from LINEITEM
where l_shipdate >= '1998-01-01' and l_shipdate < '1998-04-01'
group by l_suppkey;

-- Original query
explain analyse
select s_suppkey, s_name, s_address, s_phone, total_revenue
                from SUPPLIER, revenue15
                where s_suppkey = supplier_no and total_revenue = (
                    select max(total_revenue)
                    from revenue15
                )
                order by s_suppkey;


-- Window function query
explain analyse
select s_suppkey, s_name, s_address, s_phone, total_revenue
from
    (
        select s_suppkey, s_name, s_address, s_phone, total_revenue ,
               row_number() over (order by total_revenue desc) rn
        from revenue15, SUPPLIER
        where s_suppkey = supplier_no
    ) t
where rn = 1
order by s_suppkey;


-- Produced by the SQL rewriting tool
explain analyse
SELECT  S_SUPPKEY,  S_NAME,  S_ADDRESS,  S_PHONE,  TOTAL_REVENUE
FROM  (
          SELECT main_subquery.S_SUPPKEY, main_subquery.S_NAME, main_subquery.S_ADDRESS, main_subquery.S_PHONE, main_subquery.TOTAL_REVENUE, RN
          FROM (
                   SELECT     S_SUPPKEY,     S_NAME,     S_ADDRESS,     S_PHONE,     TOTAL_REVENUE
                   FROM revenue15 T,  SUPPLIER
                   WHERE S_SUPPKEY = SUPPLIER_NO
               ) main_subquery
                   JOIN (
              SELECT  MAX(TOTAL_REVENUE) min_TOTAL_REVENUE,  1 RN
              FROM
                  (
                      SELECT     S_SUPPKEY,     S_NAME,     S_ADDRESS,     S_PHONE,     TOTAL_REVENUE
                      FROM revenue15 T,  SUPPLIER
                      WHERE  S_SUPPKEY = SUPPLIER_NO
                  ) winfun_subquery
          ) win_subquery_1 ON win_subquery_1.min_TOTAL_REVENUE = main_subquery.TOTAL_REVENUE
      ) T
WHERE RN = 1
ORDER BY S_SUPPKEY;


drop view revenue15;




-- q17
-- Original query
        explain analyse
select sum(l_extendedprice) / 7.0 as avg_yearly
from LINEITEM, PART
where p_partkey = l_partkey and p_brand = 'Brand#23' and
        p_container = 'MED BOX' and
        l_quantity < (
        select 0.2 * avg(l_quantity)
        from LINEITEM
        where l_partkey = p_partkey
    );

-- Window function query
        explain analyse
select sum(l_extendedprice) / 7.0 as avg_yearly
from part
         join (
    select l_extendedprice, l_quantity, l_partkey, 0.2 * avg(l_quantity) over (
        partition by l_partkey) avg_l_quantity
    from lineitem
) t on l_quantity < avg_l_quantity and p_partkey = l_partkey
where p_brand = 'Brand#23' and
        p_container = 'MED BOX';

-- Produced by the SQL rewriting tool
        explain analyse
SELECT
        SUM (L_EXTENDEDPRICE) / 7.0 AS AVG_YEARLY
FROM
    PART
        JOIN (
        SELECT
            L_EXTENDEDPRICE,
            L_QUANTITY,
            L_PARTKEY,
            0.2 * AVG_L_QUANTITY AVG_L_QUANTITY
        FROM
            (
                SELECT
                    main_subquery.L_EXTENDEDPRICE,
                    main_subquery.L_QUANTITY,
                    main_subquery.L_PARTKEY,
                    AVG_L_QUANTITY
                FROM
                    (
                        SELECT
                            L_EXTENDEDPRICE,
                            L_QUANTITY,
                            L_PARTKEY
                        FROM
                            LINEITEM
                    ) main_subquery CROSS
                                        JOIN LATERAL (
                        SELECT
              AVG(L_QUANTITY) AVG_L_QUANTITY
            FROM
              (
                SELECT
                  L_EXTENDEDPRICE,
                  L_QUANTITY,
                  L_PARTKEY
                FROM
                  LINEITEM
              ) winfun_subquery
            WHERE
              (
                winfun_subquery.L_PARTKEY = main_subquery.L_PARTKEY
              )
          ) win_subquery_1
            ) T
    ) T ON L_QUANTITY < AVG_L_QUANTITY
        AND P_PARTKEY = L_PARTKEY
WHERE
        P_BRAND = 'Brand#23'
  AND P_CONTAINER = 'MED BOX';






