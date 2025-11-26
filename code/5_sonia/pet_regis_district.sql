drop view if exists v_1pet_regis_district;

create view v_1pet_regis_district as
select
	l.loc_id,
    l.city,
    l.district,
    sum(greatest(ifnull(pr.regis_count, 0) - ifnull(pr.removal_count, 0), 0)) as pet_total
from pet_regis as pr
join location as l
    on pr.loc_id = l.loc_id
group by l.city, l.district,l.loc_id;
