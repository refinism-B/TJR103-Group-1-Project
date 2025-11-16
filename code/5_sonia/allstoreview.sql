drop view if exists v_2fact_store_all;

create view v_2fact_store_all as

# hospital
select
    h.id,
    h.name,
    h.buss_status,
    h.loc_id,
    h.address,
    h.phone,
    h.op_hours,
    h.category_id,
    h.rating,
    h.rating_total,
    h.newest_review,
    h.longitude,
    h.latitude,
    h.map_url,
    h.website,
    h.place_id,
    h.update_time
from hospital as h

union all
# hotel
select
    t.id,
    t.name,
    t.buss_status,
    t.loc_id,
    t.address,
    t.phone,
    t.op_hours,
    t.category_id,
    t.rating,
    t.rating_total,
    t.newest_review,
    t.longitude,
    t.latitude,
    t.map_url,
    t.website,
    t.place_id,
    t.update_time
from hotel as t

union all
# restaurant
select
    r.id,
    r.name,
    r.buss_status,
    r.loc_id,
    r.address,
    r.phone,
    r.op_hours,
    r.category_id,
    r.rating,
    r.rating_total,
    r.newest_review,
    r.longitude,
    r.latitude,
    r.map_url,
    r.website,
    r.place_id,
    r.update_time
from restaurant as r

union all
# salon
select
    s.id,
    s.name,
    s.buss_status,
    s.loc_id,
    s.address,
    s.phone,
    s.op_hours,
    s.category_id,
    s.rating,
    s.rating_total,
    s.newest_review,
    s.longitude,
    s.latitude,
    s.map_url,
    s.website,
    s.place_id,
    s.update_time
from salon as s

union all
# supplies
select
    p.id,
    p.name,
    p.buss_status,
    p.loc_id,
    p.address,
    p.phone,
    p.op_hours,
    p.category_id,
    p.rating,
    p.rating_total,
    p.newest_review,
    p.longitude,
    p.latitude,
    p.map_url,
    p.website,
    p.place_id,
    p.update_time
from supplies as p

union all
# shelter
select
    sh.id,
    sh.name,
    sh.buss_status,
    sh.loc_id,
    sh.address,
    sh.phone,
    sh.op_hours,
    sh.category_id,
    sh.rating,
    sh.rating_total,
    sh.newest_review,
    sh.longitude,
    sh.latitude,
    sh.map_url,
    sh.website,
    sh.place_id,
    sh.update_time
from shelter as sh;

