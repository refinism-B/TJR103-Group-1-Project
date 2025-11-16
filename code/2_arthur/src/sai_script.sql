CREATE OR REPLACE VIEW v_store_attraction_index AS
SELECT 
    mp.loc_id,
    mp.city,
    mp.district,
    c.category_id,
    c.category_count,
    round(mp.market_potential, 2) AS 'market_potential', -- 市場潛力
    round((c.category_count * mp.sum_pet / 10000), 2) AS store_saturation, -- 店家密集度
    round(n.pet_industry_maturity, 2) AS 'pet_industry_maturity', -- 寵物產業成熟度
    round(pow(mp.market_potential, 0.4) * pow((c.category_count * mp.sum_pet / 10000), 0.3) * pow(n.pet_industry_maturity, 0.3),2 ) as 'sai'
FROM 
    (
        SELECT 
            p.loc_id,
            ANY_VALUE(l.city) AS city,
            ANY_VALUE(l.district) AS district,
            SUM(p.regis_count) AS  sum_pet,
            POW(
                (
                    (SUM(p.regis_count) / ANY_VALUE(l.area)) *
                    ((SUM(p.regis_count) / ANY_VALUE(l.population)) * 1000) *
                    (ANY_VALUE(l.population) / ANY_VALUE(l.area))
                ),
                1/3
            ) AS market_potential
        FROM pet_regis p
        JOIN location l ON p.loc_id = l.loc_id
        GROUP BY p.loc_id
    ) AS mp
LEFT JOIN (
    SELECT 
        loc_id,
        category_id,
        COUNT(*) AS category_count
    FROM v_store_loc_cat_id
    GROUP BY loc_id, category_id
) AS c
ON mp.loc_id = c.loc_id
LEFT JOIN (
    SELECT 
    	loc_id,
        (city_score_weighted / 10) as pet_industry_maturity
    FROM v_001single_district_scores_ranked vsdsr 
) AS n
ON mp.loc_id = n.loc_id
ORDER BY mp.loc_id, c.category_id;



-- 建立只有店家loc_id跟category_id的view
create view v_store_loc_cat_id as 
select
	loc_id,
	category_id
from
	hospital h
union all
select
	loc_id,
	category_id
from
	hotel h
union all
select
	loc_id,
	category_id
from
	restaurant r
union all
select
	loc_id,
	category_id
from
	salon s
union all
select
	loc_id,
	category_id
from
	shelter s
union all
select
	loc_id,
	category_id
from
	supplies s ;

