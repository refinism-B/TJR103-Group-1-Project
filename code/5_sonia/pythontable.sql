#用來存Python算出來的參數


create table if not exists A_4agg_district_cat_m #城市行政區類別的 m 值
( loc_id varchar(64) not null,
  city varchar(64) not null,
  district varchar(64) not null,
  category_id int not null,
  m_city_district_cat decimal(12,4) not null,
  primary key (city, district, category_id)
);

create table if not exists A_7category_score_norm #城市跟六都的標準化分數
( loc_id varchar(64) not null,
  city varchar(64) not null,
  district varchar(64) not null,
  category_id int not null,
  norm_city decimal(12,2) not null,
  norm_metro decimal(12,2) not null,
  primary key (loc_id,city, district, category_id)

)

show tables;

