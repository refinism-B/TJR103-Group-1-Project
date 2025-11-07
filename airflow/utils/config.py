from datetime import date, datetime, timedelta

CITY_NAME_CODE_DICT = {
    "A": "新北市",
    "V": "臺北市",
    "C": "桃園市",
    "S": "臺中市",
    "U": "臺南市",
    "W": "高雄市"
}


STORE_TYPE_CODE_DICT = {
    "寵物美容": "sal",
    "寵物餐廳": "res",
    "寵物用品": "supl"
}

STORE_TYPE_ENG_CH_DICT = {
    "寵物美容": "salon",
    "寵物餐廳": "restaurant",
    "寵物用品": "supplies"
}

WORDS_REPLACE_FROM_ADDRESS = {
    "区": "區",
    "霧峯": "霧峰",
    "中壢市": "中壢區",
    "省": "",
    "萬裏": "萬里",
    "區區": "區"
}


GSEARCH_CITY_CODE = {
    "新北市": "TPE",
    "桃園市": "TYU",
    "臺中市": "TCH",
    "臺南市": "TNA",
    "高雄市": "KSH"
}


PET_REGIS_COLUMNS_NAME = [
    "area_id",
    "district",
    "登記單位數",
    "regis_count",
    "removal_count",
    "轉讓數",
    "變更數",
    "絕育數",
    "絕育除戶數",
    "免絕育數",
    "免絕育除戶數",
    "animal",
    "date",
    "city",
    "update_date"
]


PET_REGIS_DROP_COLUMNS = [
    "area_id",
    "登記單位數",
    "轉讓數",
    "變更數",
    "絕育數",
    "絕育除戶數",
    "免絕育數",
    "免絕育除戶數"
]


PET_REGIS_FINAL_COLUMNS = [
    "loc_id",
    "date",
    "animal",
    "regis_count",
    "removal_count",
    "update_date"
]


STORE_DROP_KEY_WORDS = [
    "停業",
    "歇業",
    "暫停營業",
    "暫停服務",
    "停止營業",
    "停止服務"
]


ADDRESS_DROP_KEYWORDS = [
    "路",
    "街",
    "巷",
    "弄",
    "段",
    "道"
]


GMAP_INFO_SEARCH_FINAL_COLUMNS = [
    "id", 'name',
    'buss_status',
    'loc_id',
    'address',
    'phone',
    "op_hours",
    'category_id',
    'rating',
    'rating_total',
    'newest_review',
    'longitude',
    'latitude',
    'map_url',
    'website',
    'place_id',
    'update_time'
]
