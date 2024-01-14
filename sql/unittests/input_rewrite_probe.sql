select sum(l_extendedprice) / 7.0 as avg_yearly
from part
         join (
    select l_extendedprice, l_quantity, l_partkey,
           avg(l_quantity) over (partition by l_partkey) avg_l_quantity
    from lineitem
) t on l_quantity < 0.2 * avg_l_quantity and p_partkey = l_partkey
where p_brand = 'Brand#23' and
        p_container = 'MED BOX';