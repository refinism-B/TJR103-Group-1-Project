drop view if exists v_6category_weight;

create view v_6category_weight as
select 1 as category_id, 0.27 as weight   # hospital_general
union all select 7, 0.22                  # hospital_24h
union all select 5, 0.13                  # supplies
union all select 3, 0.11                  # hotel
union all select 2, 0.09                  # restaurant
union all select 4, 0.15                  # salon
union all select 6, 0.03;                 # shelter
