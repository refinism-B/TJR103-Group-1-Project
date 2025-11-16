

drop view if exists v_4store_score;

create view v_4store_score as
select
    f.id,             # 店家id
    f.loc_id,         # 行政區位置id
    f.category_id,    # 類別
    l.city,           # 城市
    l.district,       # 行政區
    f.rating,         # 星等
    f.rating_total,   # 評論數

    # 行政區類別的動態平滑 m（沒有資料先當 0）
    ifnull(m.m_city_district_cat, 0) as m,

    # 行政區類別平均星等（沒有就用店家自己的 rating 當備援）
    ifnull(s.district_cat_avg, f.rating) as district_cat_avg,

    # 時間分數（0.5 + 0.5 * hours / 168）,先把 hours 限制在 0-168
    (0.5 + 0.5 * (least(greatest(ifnull(f.op_hours, 0), 0), 168) / 168.0)) as availability_score,

    # bayes（防止分母 = 0）
    (
      (f.rating / 5.0) *
        (
          ifnull(f.rating_total, 0)
          / ifnull(
              nullif(ifnull(f.rating_total, 0) + ifnull(m.m_city_district_cat, 0), 0),
              1
            )
        )
      +
      (ifnull(s.district_cat_avg, f.rating) / 5.0) *
        (
          ifnull(m.m_city_district_cat, 0)
          / ifnull(
              nullif(ifnull(f.rating_total, 0) + ifnull(m.m_city_district_cat, 0), 0),
              1
            )
        )
    ) as bayes_part,

    # 單店總分=bayes_part × availability_score
    (
      (
        (f.rating / 5.0) *
          (
            ifnull(f.rating_total, 0)
            / ifnull(
                nullif(ifnull(f.rating_total, 0) + ifnull(m.m_city_district_cat, 0), 0),
                1
              )
          )
        +
        (ifnull(s.district_cat_avg, f.rating) / 5.0) *
          (
            ifnull(m.m_city_district_cat, 0)
            / ifnull(
                nullif(ifnull(f.rating_total, 0) + ifnull(m.m_city_district_cat, 0), 0),
                1
              )
          )
      )
      * (0.5 + 0.5 * (least(greatest(ifnull(f.op_hours, 0), 0), 168) / 168.0))
    ) as store_score

from v_2fact_store_all as f

# loc_id 對到的city/district
join location as l
  on f.loc_id = l.loc_id

# m：loc_id + category_id 對 a_4agg_district_cat_m
left join A_4agg_district_cat_m as m
  on f.loc_id      = m.loc_id
 and f.category_id = m.category_id

# 行政區平均星等：同樣用loc_id + category_id 對 v_3district_cat_stats
left join v_3district_cat_stats as s
  on f.loc_id      = s.loc_id
 and f.category_id = s.category_id;

select category_id, count(*) as cnt
from v_4store_score
group by category_id
order by category_id;

select *
from v_4store_score
where category_id = 6
limit 20;

