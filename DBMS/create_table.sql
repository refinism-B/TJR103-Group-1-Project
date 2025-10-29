DROP TABLE IF EXISTS Hospital;
CREATE TABLE Hospital(
	hospital_id VARCHAR(12) NOT NULL COMMENT '醫院id',
	name varchar(100) NOT NULL COMMENT '醫院名稱',
	address varchar(100) NOT NULL COMMENT '醫院地址',
	location_id varchar(12) NOT NULL COMMENT '醫院所在行政區編號',
	category_id INT NOT NULL COMMENT '醫院所屬類別編號',
	business_status BOOLEAN NOT NULL COMMENT '營業狀態',
	opening_hour DECIMAL(3,1) NOT NULL COMMENT '一周營業時長',
	rating DECIMAL(2,1) NOT NULL COMMENT '評論星等',
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
) COMMENT = '儀錶板資料表'