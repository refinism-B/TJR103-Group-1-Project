drop view if exists v_001single_district_scores_ranked;

create view v_001single_district_scores_ranked as
with district_base as (
    # 每個行政區的市內總分（把各類別 weighted_city加總 再到 0~10，取到小數點第2位）
    select
        w.loc_id,
        w.city,
        w.district,
        round(sum(w.weighted_city) * (10.0 / 9.5), 2) as city_score_weighted
    from v_8weighted_category_scores as w
    group by
        w.loc_id,
        w.city,
        w.district
)
select
    d.loc_id,
    d.city,
    d.district,
    d.city_score_weighted,

    # 市內排名：同一個 city 內，市內總分由高到低
    rank() over (
        partition by d.city
        order by d.city_score_weighted desc
    ) as city_rank,

    # 六都總排名：所有行政區一起比市內總分
    rank() over (
        order by d.city_score_weighted desc
    ) as metro_rank

from district_base as d;

