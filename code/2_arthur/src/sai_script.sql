CREATE OR REPLACE VIEW v_store_attraction_index AS
SELECT
    mp.loc_id,
    mp.city,
    mp.district,
    c.category_id,
    c.category_count,

    -- 市場潛力 normalization
    ROUND(
        (mp.market_potential - mp_stats.min_mp) /
        (mp_stats.max_mp - mp_stats.min_mp),
    2) AS market_potential_norm,

    -- 原始店家飽和度
    ROUND(
        (c.category_count / (mp.sum_pet / 10000)),
    2) AS store_saturation_raw,

    -- 正規化反轉版 store saturation
    ROUND(
        1 - GREATEST(
                (
                    (c.category_count / (mp.sum_pet / 10000))
                    - sat_stats.min_sat
                ) / 
                (sat_stats.max_sat - sat_stats.min_sat),
        0),
    2) AS store_saturation_norm,

    -- 寵物產業成熟度
    ROUND(n.pet_industry_maturity, 2) AS pet_industry_maturity,

    -- ⭐ SAI：三項綜合指標
    ROUND(
        POW(
            (mp.market_potential - mp_stats.min_mp) /
            (mp_stats.max_mp - mp_stats.min_mp),
        0.4)
        *
		POW(
		    LEAST(
		        GREATEST(
		            1 - (
		                    (
		                        (c.category_count / (mp.sum_pet / 10000)) 
		                        - sat_stats.min_sat
		                    )
		                    / (sat_stats.max_sat - sat_stats.min_sat)
		                )
		        , 0)
		    , 1)
		, 0.3)
        *
        POW(n.pet_industry_maturity, 0.3)
    , 2) AS sai,

    -- SAI 放大版本（10x）
    ROUND(
        (
            POW(
                (mp.market_potential - mp_stats.min_mp) /
                (mp_stats.max_mp - mp_stats.min_mp),
            0.4)
            *
			POW(
			    LEAST(
			        GREATEST(
			            1 - (
			                    (
			                        (c.category_count / (mp.sum_pet / 10000)) 
			                        - sat_stats.min_sat
			                    )
			                    / (sat_stats.max_sat - sat_stats.min_sat)
			                )
			        , 0)
			    , 1)
			, 0.3)
            *
            POW(n.pet_industry_maturity, 0.3)
        ) * 10
    , 2) AS sai_10x

FROM
    (
        -- 每個 loc_id 的市場潛力
        SELECT 
            p.loc_id,
            ANY_VALUE(l.city) AS city,
            ANY_VALUE(l.district) AS district,
            SUM(p.regis_count) AS sum_pet,
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

-- 市場潛力的 global min/max
CROSS JOIN (
    SELECT 
        MIN(market_potential) AS min_mp,
        MAX(market_potential) AS max_mp
    FROM (
        SELECT 
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
    ) AS t1
) AS mp_stats

-- 店家數
LEFT JOIN (
    SELECT
        loc_id,
        category_id,
        COUNT(*) AS category_count
    FROM v_store_loc_cat_id
    GROUP BY loc_id, category_id
) AS c
ON mp.loc_id = c.loc_id

-- 寵物產業成熟度
LEFT JOIN (
    SELECT
        loc_id,
        AVG(city_score_weighted / 10) AS pet_industry_maturity
    FROM v_001single_district_scores_ranked
    GROUP BY loc_id
) AS n
ON mp.loc_id = n.loc_id

-- 店家飽和度 min/max（全球）
CROSS JOIN (
    SELECT
        MIN(store_saturation_raw) AS min_sat,
        MAX(store_saturation_raw) AS max_sat
    FROM (
        SELECT 
            c.loc_id,
            (c.category_count / (mp.sum_pet / 10000)) AS store_saturation_raw
        FROM (
            SELECT 
                loc_id,
                category_id,
                COUNT(*) AS category_count
            FROM v_store_loc_cat_id
            GROUP BY loc_id, category_id
        ) AS c
        JOIN (
            SELECT 
                p.loc_id,
                SUM(p.regis_count) AS sum_pet
            FROM pet_regis p
            GROUP BY p.loc_id
        ) AS mp
        ON mp.loc_id = c.loc_id
    ) AS t2
) AS sat_stats

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

