-- PostgreSQL 

-- Q2
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





-- Q11
        explain analyse
select ps_partkey, sum(ps_supplycost * ps_availqty) as value
from PARTSUPP, SUPPLIER, NATION
where ps_suppkey = s_suppkey and s_nationkey = n_nationkey and n_name = 'GERMANY'
group by ps_partkey
having sum(ps_supplycost * ps_availqty) > (
    select sum(ps_supplycost * ps_availqty) * 0.00001
    from PARTSUPP, SUPPLIER, NATION
    where ps_suppkey = s_suppkey and s_nationkey = n_nationkey and n_name = 'GERMANY'
)
order by value desc;

-- explain analyse
select ps_partkey, s1
from (
         select distinct ps_partkey,
                         sum(ps_supplycost * ps_availqty) over (partition by ps_partkey) s1,
                         sum(ps_supplycost * ps_availqty) over () * 0.00001 s2
         from PARTSUPP, SUPPLIER, NATION
         where ps_suppkey = s_suppkey and s_nationkey = n_nationkey and n_name = 'GERMANY'
     ) t
where s1 > s2
order by s1 desc;


        explain analyse
SELECT PS_PARTKEY, S1
FROM
    (
        SELECT DISTINCT main_subquery.PS_PARTKEY,  S1, S2
        FROM
            (
                SELECT
                    DISTINCT PS_PARTKEY,
                             VALUE
                FROM
                    (
                        SELECT
                                PS_SUPPLYCOST * PS_AVAILQTY AS VALUE,
                                PS_PARTKEY PS_PARTKEY
                        FROM
                            PARTSUPP,
                            SUPPLIER,
                            NATION
                        WHERE
                                PS_SUPPKEY = S_SUPPKEY
                          AND S_NATIONKEY = N_NATIONKEY
                          AND N_NAME = 'GERMANY'
                    ) T
            ) main_subquery CROSS
                                JOIN LATERAL (
                SELECT
          SUM(VALUE) S1
        FROM
          (
            SELECT
              DISTINCT PS_PARTKEY,
              VALUE
            FROM
              (
                SELECT
                  PS_SUPPLYCOST * PS_AVAILQTY AS VALUE,
                  PS_PARTKEY PS_PARTKEY
                FROM
                  PARTSUPP,
                  SUPPLIER,
                  NATION
                WHERE
                  PS_SUPPKEY = S_SUPPKEY
                  AND S_NATIONKEY = N_NATIONKEY
                  AND N_NAME = 'GERMANY'
              ) T
          ) winfun_subquery
        WHERE
          (
            winfun_subquery.PS_PARTKEY = main_subquery.PS_PARTKEY
          )
      ) win_subquery_1 CROSS
                                JOIN LATERAL (
                SELECT
          SUM(VALUE) S2
        FROM
          (
            SELECT
              DISTINCT PS_PARTKEY,
              VALUE
            FROM
              (
                SELECT
                  PS_SUPPLYCOST * PS_AVAILQTY AS VALUE,
                  PS_PARTKEY PS_PARTKEY
                FROM
                  PARTSUPP,
                  SUPPLIER,
                  NATION
                WHERE
                  PS_SUPPKEY = S_SUPPKEY
                  AND S_NATIONKEY = N_NATIONKEY
                  AND N_NAME = 'GERMANY'
              ) T
          ) winfun_subquery
      ) win_subquery_2
    ) T
WHERE
        S1 > S2 * 0.00001
ORDER BY
    S1 DESC;








-- q15
create view revenue15 (supplier_no, total_revenue) as
select l_suppkey, sum(l_extendedprice * (1 - l_discount))
from LINEITEM
where l_shipdate >= '1998-01-01' and l_shipdate < '1998-04-01'
group by l_suppkey;

        explain analyse select s_suppkey, s_name, s_address, s_phone, total_revenue
                        from SUPPLIER, revenue15
                        where s_suppkey = supplier_no and total_revenue = (
                            select max(total_revenue)
                            from revenue15
                        )
                        order by s_suppkey;


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








-- q22
        explain analyse
select cntrycode,
       count(*) as numcust,
       sum(c_acctbal) as totacctbal
from (
         select substring(c_phone, 1, 2) as cntrycode, c_acctbal
         from CUSTOMER
         where substring(c_phone, 1, 2) in ('13', '31', '23', '29', '30', '18', '17') and
                 c_acctbal > (
                 select avg(c_acctbal)
                 from CUSTOMER
                 where c_acctbal > 0.00 and
                         substring(c_phone, 1, 2) in ('13', '31', '23', '29', '30', '18', '17')
             ) and not exists (
                 select *
                 from ORDERS
                 where o_custkey = c_custkey
             )
     ) as custsale
group by cntrycode
order by cntrycode;

        explain analyse
select cntrycode,
       count(*) as numcust,
       sum(c_acctbal) as totacctbal
from (
         select c_custkey, c_acctbal, avg(c_acctbal) over () avg_c_acctbal, substring(c_phone, 0, 3) cntrycode
         from CUSTOMER
         where c_acctbal > 0.00 and
                 substring(c_phone, 0, 3) in ('13', '31', '23', '29', '30', '18', '17')
     ) t
where c_acctbal > avg_c_acctbal and not exists (
        select *
        from ORDERS
        where o_custkey = c_custkey
    )
group by cntrycode
order by cntrycode;



        explain analyse
SELECT
    CNTRYCODE,
    COUNT (*) AS NUMCUST,
    SUM (C_ACCTBAL) AS TOTACCTBAL
FROM
    (
        SELECT
            main_subquery.C_CUSTKEY,
            main_subquery.C_ACCTBAL,
            AVG_C_ACCTBAL,
            main_subquery.CNTRYCODE
        FROM
            (
                SELECT
                    C_CUSTKEY,
                    C_ACCTBAL,
                    substring(c_phone, 0, 3) CNTRYCODE
                FROM
                    CUSTOMER
                WHERE
                        C_ACCTBAL > 0.00
                  AND substring(c_phone, 0, 3) IN (
                                                   '13', '31', '23', '29', '30', '18', '17'
                    )
            ) main_subquery CROSS
                                JOIN LATERAL (
                SELECT
          AVG(C_ACCTBAL) AVG_C_ACCTBAL
        FROM
          (
            SELECT
              C_CUSTKEY,
              C_ACCTBAL,
              SUBSTR (C_PHONE, 1, 2) CNTRYCODE
            FROM
              CUSTOMER
            WHERE
              C_ACCTBAL > 0.00
              AND substring(c_phone, 0, 3) IN (
                '13', '31', '23', '29', '30', '18', '17'
              )
          ) winfun_subquery
      ) win_subquery_1
    ) T
WHERE
        C_ACCTBAL > AVG_C_ACCTBAL
  AND NOT EXISTS (
        SELECT
            *
        FROM
            ORDERS
        WHERE
                O_CUSTKEY = C_CUSTKEY
    )
GROUP BY
    CNTRYCODE
ORDER BY
    CNTRYCODE;
