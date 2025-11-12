

"""
本檔案存放常用的、固定的list或dict資料。
如需擴充、修改可直接在此修改。
"""


# 寵物登記數資料
# 用於寵物登記數的縣市代碼轉換
CITY_NAME_CODE_DICT = {
    "A": "新北市",
    "V": "臺北市",
    "C": "桃園市",
    "S": "臺中市",
    "U": "臺南市",
    "W": "高雄市"
}

# 寵物登記資料的正確欄位名稱（中文為會捨去的欄位）
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
    "date",
    "animal",
    "city",
    "update_date"
]

# 寵物登記數資料中需去除的欄位
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

# 寵物登記數資料最終的欄位名（含順序）
PET_REGIS_FINAL_COLUMNS = [
    "loc_id",
    "date",
    "animal",
    "regis_count",
    "removal_count",
    "update_date"
]


# gmap搜尋六都取得店家place id
# 用於商店類別資料的id前綴字串轉換
STORE_TYPE_CODE_DICT = {
    "寵物美容": "sal",
    "寵物餐廳": "res",
    "寵物用品": "supl"
}

# 用於商店類別資料的中英文轉換
STORE_TYPE_ENG_CH_DICT = {
    "寵物美容": "salon",
    "寵物餐廳": "restaurant",
    "寵物用品": "supplies"
}

# 用於搜尋六都商店時的代號（新北範圍包含台北）
GSEARCH_CITY_CODE = {
    "新北市": "TPE",
    "桃園市": "TYU",
    "臺中市": "TCH",
    "臺南市": "TNA",
    "高雄市": "KSH"
}


# gmap搜尋店家詳細資料
# 透過gmap搜尋取得店家資訊後的欄位名
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

# 清洗商店詳細資訊時，地址中需要轉換的字串
WORDS_REPLACE_FROM_ADDRESS = {
    "区": "區",
    "霧峯": "霧峰",
    "中壢市": "中壢區",
    "省": "",
    "萬裏": "萬里",
    "區區": "區"
}

# 在店家資訊的標題（店名）中隱含關閉、不提供服務的字樣，將其從資料中去除
STORE_DROP_KEY_WORDS = [
    "停業",
    "歇業",
    "暫停營業",
    "暫停服務",
    "停止營業",
    "停止服務"
]

# 用於店家資訊清洗地區欄位時使用
ADDRESS_DROP_KEYWORDS = [
    "路",
    "街",
    "巷",
    "弄",
    "段",
    "道"
]


# 爬取地區面積資料
# 地區面積資料連結
LOCATION_AREA_URL = "https://www.ris.gov.tw/info-popudata/app/awFastDownload/file/y0s6-00000.xls/y0s6/00000/"

# 地區面積表的欄位
LOCATION_AREA_COLUMNS = ["location", "population", "area", "density"]
