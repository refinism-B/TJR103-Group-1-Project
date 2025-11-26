drop view if exists v_5category_raw_district;

create view v_5category_raw_district as
select
	s.loc_id,
    s.city,
    s.district,
    s.category_id,
    sum(ifnull(s.store_score, 0)) as sum_store_score,     # 類別內所有店家的分數總和
    p.pet_total,
    # 依寵物數標準化（每1萬隻為單位）
    # (greatest(ifnull(p.pet_total, 0), 1)確保分母不為 0（至少1）
    sum(ifnull(s.store_score, 0)) / (greatest(ifnull(p.pet_total, 0), 1) / 10000.0) as category_raw_score 
from v_4store_score as s
left join v_1pet_regis_district as p
    on s.city = p.city
   and s.district = p.district
   and s.loc_id = p.loc_id
group by s.loc_id,s.city, s.district, s.category_id, p.pet_total;
