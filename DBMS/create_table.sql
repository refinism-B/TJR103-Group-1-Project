DROP TABLE IF EXISTS Hospital;

CREATE TABLE Hospital(
	hospital_id varchar(12) not null COMMENT "醫院id",
	name varchar(100) not null COMMENT "醫院名稱",
	address varchar(100) not null COMMENT "醫院地址",
	location_id varchar(12) not null COMMENT "醫院所在行政區編號",
	category_id varchar(12) not null COMMENT "醫院所屬類別編號",
	opening_hour bool not null DEFAULT 0 COMMENT "是否營業24小時",
	# constraint
	primary key (hospital_id)
) comment = "醫院基本資料表";