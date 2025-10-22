import googlemaps
from datetime import datetime
from bs4 import BeautifulSoup




def get_key(api_key_path):
    """提供key檔案路徑，自動讀取內容並回傳"""
    with open(api_key_path, "r", encoding="UTF-8") as f:
        api_key = f.read()
    
    return api_key


def trans_unix_to_date(timestamp):
    """將UNIX電腦時間轉換為西元年月日"""
    # 將時間戳轉成 datetime 物件
    dt = datetime.fromtimestamp(timestamp)

    # 格式化成 "年-月-日"
    date_str = dt.strftime('%Y-%m-%d')

    return date_str


def get_place_id(api_key: str, key_word: str):
    """提供關鍵字（最好是店名+地址），透過gmap搜尋取得place_id"""
    gmaps = googlemaps.Client(key=api_key)
    search_result = gmaps.places(query=key_word, language='zh-TW')
    if len(search_result["results"]) != 0:
        place_id = search_result.get("results", {})[0]["place_id"]
        return place_id
    else:
        return None



def newest_review_date(review_list: list):
    """提供gmaps回傳的評論列表，回傳列表中最新的評論時間"""
    for i in review_list:
        time = 0
        if i["time"] > 0:
            time = i["time"]
    date = trans_unix_to_date(time)

    return date


def gmap_info(ori_name, api_key, place_id):
    """提供place_id，回傳名稱、營業狀態、營業時間、gmap評分、經緯度、gmap網址、最新評論日期"""
    if place_id != None:
        gmaps = googlemaps.Client(key=api_key)
        detail = gmaps.place(place_id=place_id, language='zh-TW')
        name = detail.get("result", {}).get("name", None)
        business_status = detail.get("result", {}).get("business_status", None)

        if detail["result"]["formatted_address"]:
            address = detail.get("result", {}).get("formatted_address", None)
        elif detail['result']['adr_address']:
            address = BeautifulSoup(detail.get('result', {}).get('adr_address', None), "html.parser").text
        else:
            address = None
        
        phone = detail.get("result", {}).get("formatted_phone_number", None)
        if phone != None:
            phone = phone.replace(" ", "")

        opening_hours = detail.get("result", {}).get("opening_hours", {}).get("weekday_text", None)
        rating = detail.get("result", {}).get('rating', None)
        rating_total = detail.get("result", {}).get("user_ratings_total", None)
        # geocode = if_exist(detail["result"]['geometry']['location'])
        longitude = detail.get("result", {}).get('geometry', {}).get('location', {}).get('lng', None)
        latitude = detail.get("result", {}).get('geometry', {}).get('location', {}).get('lat', None)
        map_url = detail.get("result", {}).get("url", None)
        review_list = detail.get("result", {}).get('reviews', None)
        
        if review_list != None:
            newest_review = newest_review_date(review_list)
        else:
            newest_review = None

        place_info = {
            "name":name,
            "place_id":place_id,
            "business_status":business_status,
            "address":address,
            "phone":phone,
            "opening_hours":opening_hours,
            "rating":rating,
            "rating_total":rating_total,
            "longitude":longitude,
            "latitude":latitude,
            "map_url":map_url,
            "newest_review":newest_review
        }
    else:
        place_info = {
            "name":ori_name,
            "place_id":None,
            "business_status":None,
            "address":None,
            "phone":None,
            "opening_hours":None,
            "rating":None,
            "rating_total":None,
            "longitude":None,
            "latitude":None,
            "map_url":None,
            "newest_review":None
        }

    return place_info


def get_place_dict(api_key_path, name: str, address: str):
    """帶入api_key.txt檔案路徑和「命稱」、「地址」字串，能自動搜尋並返回資訊。
       返回資訊為字典，可直接轉換成DataFrame。"""
    key_word = name + " " + address

    api_key = get_key(api_key_path=api_key_path)
    place_id = get_place_id(api_key=api_key, key_word=key_word)
    place_dict = gmap_info(ori_name=name, api_key=api_key, place_id=place_id)

    return place_dict