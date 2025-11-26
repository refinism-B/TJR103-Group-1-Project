
drop view if exists v_3district_cat_stats;

create view v_3district_cat_stats as
select
	l.loc_id,
    l.city,
    l.district,
    c.category_id,
    avg(ifnull(f.rating, 0)) as district_cat_avg,
    count(f.id) as n_district_cat
from location as l
cross join category as c
left join v_2fact_store_all as f
    on f.loc_id = l.loc_id
   and f.category_id = c.category_id
where l.city is not null              # 擋掉對不到地區的店家
  and l.district is not null
group by l.loc_id,l.city, l.district, c.category_id;
