DROP TABLE IF EXISTS Hospital;
CREATE TABLE Hospital(
	hospital_id VARCHAR(12) NOT NULL COMMENT '醫院id',
	name varchar(100) NOT NULL COMMENT '醫院名稱',
	address varchar(100) NOT NULL COMMENT '醫院地址',
	location_id varchar(12) NOT NULL COMMENT '醫院所在行政區編號',
	category_id INT NOT NULL COMMENT '醫院所屬類別編號',
	business_status BOOLEAN NOT NULL COMMENT '營業狀態',
	opening_hour DECIMAL(3, 1) NOT NULL COMMENT '一周營業時長',
	rating DECIMAL(2, 1) NOT NULL COMMENT '評論星等',
	review_count INT NOT NULL COMMENT '評論總數',
	-- CONSTRAINT
	primary key (hospital_id)
) COMMENT = '醫院基本資料表';
# 
DROP TABLE IF EXISTS Location;
CREATE TABLE Location(
	location_id VARCHAR(12) NOT NULL COMMENT '市和區的id',
	city VARCHAR(15) NOT NULL COMMENT '直轄市',
	district VARCHAR(100) NOT NULL COMMENT '區',
	area DECIMAL(6, 2) NOT NULL COMMENT '面積，單位平方公里',
	population int NOT NULL COMMENT '人口數',
	-- CONSTRAINT
	PRIMARY KEY (location_id),
	UNIQUE (district)
) COMMENT = '直轄市資料表';
# 
DROP TABLE IF EXISTS Category;
CREATE TABLE Category(
	category_id INT NOT NULL COMMENT '機構位於分類表的id',
	category_name VARCHAR(30) NOT NULL COMMENT '分類名稱',
	-- CONSTRAINT
	PRIMARY KEY(category_id)
) COMMENT = '機構分類資料表';
INSERT INTO Category
VALUES (1, "動物醫院"),
	(2, "寵物餐廳"),
	(3, "寵物旅館"),
	(4, "友善設施"),
	(5, "收容所");
# 
DROP TABLE IF EXISTS Dashboard;
CREATE TABLE Dashboard(
	location VARCHAR(12),
	convenience DECIMAL(2, 1)
) COMMENT = '儀錶板資料表';
#
DROP TABLE IF EXISTS Hotel;
CREATE TABLE Hotel(
	hotel_id VARCHAR(20) NOT NULL COMMENT '寵物旅館id',
	place_id VARCHAR(50) NOT NULL COMMENT 'gmap的id',
	name VARCHAR(100) NOT NULL COMMENT '寵旅名稱',
	address VARCHAR(150) NOT NULL COMMENT '寵旅地址',
	phone VARCHAR(15) COMMENT '寵旅電話',
	city VARCHAR (15) NOT NULL COMMENT '寵旅所在市',
	district VARCHAR(30) NOT NULL COMMENT '寵旅所在區',
	loc_id VARCHAR(30) NOT NULL COMMENT '市區的id',
	business_status VARCHAR(30) NOT NULL COMMENT '營業狀態',
	opening_hours INT NOT NULL COMMENT '營業時長',
	types VARCHAR(200) COMMENT '機構種類',
	rating DECIMAL(2, 1) NOT NULL COMMENT '評論星數',
	rating_total INT NOT NULL COMMENT '評論數量',
	longitude DECIMAL(9, 6) NOT NULL COMMENT '經度',
	latitude DECIMAL(9, 6) NOT NULL COMMENT '緯度',
	map_url VARCHAR(500) NOT NULL COMMENT 'gmap連結',
	website VARCHAR(500) COMMENT '寵旅官網',
	newest_review DATE COMMENT '最新評論日期',
	created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '初次寫入時間',
	updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '修改時間',
	-- CONSTRAINT
	PRIMARY KEY(hotel_id)
-- 	FOREIGN KEY(loc_id) REFERENCES location(loc_id)
) COMMENT = '寵物旅館資料表';