drop view if exists v_9district_scores_ranked;

create view v_9district_scores_ranked as
with district_base as (
    # 先算出每個行政區的市內總分、六都總分，取到小數點第二位
    select
        w.loc_id,
        w.city,
        w.district,
        round(sum(w.weighted_city), 2)  as city_score_weighted,   # 市內加權總分
        round(sum(w.weighted_metro), 2) as metro_score_weighted   # 六都加權總分
    from v_8weighted_category_scores as w
    group by
        w.loc_id,
        w.city,
        w.district
),
scored as (
    # 70% 市內 + 30% 六都，再做 10/9.5 校正成滿分 10，取到小數點第二位
    select
        d.loc_id,
        d.city,
        d.district,
        d.city_score_weighted,
        d.metro_score_weighted,
        round(
            (
              (d.city_score_weighted  * 0.7) +
              (d.metro_score_weighted * 0.3)
            ) * (10.0 / 9.5),
            2
        ) as final_score
    from district_base as d
)

select
    s.loc_id,
    s.city,
    s.district,
    s.city_score_weighted,
    s.metro_score_weighted,
    s.final_score,

    # 市內排名（同 city 內比較）
    rank() over (
      partition by s.city
      order by s.final_score desc
    ) as city_rank,

    # 六都總排名（所有行政區一起比）
    rank() over (
      order by s.final_score desc
    ) as metro_rank

from scored as s;



