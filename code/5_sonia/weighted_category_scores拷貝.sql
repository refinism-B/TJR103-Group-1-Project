
drop view if exists v_8weighted_category_scores;

create view v_8weighted_category_scores as
select
	n.loc_id,
    n.city,                       # 城市
    n.district,                   # 行政區
    n.category_id,                # 類別
    n.category_raw_score,         # 原始分
    ifnull(n.norm_city,  0.5) as norm_city,   # 保底 0.5避免 null
    ifnull(n.norm_metro, 0.5) as norm_metro,  # 保底 0.5避免 null
    w.weight,                     # 類別權重
    # 類別市內加權分、類別六都加權分（在 0.5~9.5 範圍內）
    (ifnull(n.norm_city,  0.5) * w.weight)  as weighted_city,
    (ifnull(n.norm_metro, 0.5) * w.weight)  as weighted_metro
from A_7category_score_norm as n
join v_6category_weight as w
    on n.category_id = w.category_id ;
