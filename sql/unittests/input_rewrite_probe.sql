
select *
from (
         select id, search, groupby,
                rank() over (partition by groupby order by id) rn
         from A
     ) t
where rn = 1 and  search = 200 or groupby >= 1000
order by id;