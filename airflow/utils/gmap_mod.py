import math
import time
from datetime import datetime
from typing import Optional

import googlemaps
from airflow.decorators import task
from bs4 import BeautifulSoup

"""
這個模組主要用於google map（以下簡稱gmap）套件爬取資料。

gmap套件基本用法：
1.  gmaps = googlemaps.Client(key=api_key)
    帶入金鑰建立連線。
    
2.  search_result = gmaps.place(place_id=place_id, language="zh-TW")
    place id為每個gmap地標都有的唯一代碼，輸入place id即可取得該地標資訊。
    
3.  search_result = gmaps.places(query=keyword, language="zh-TW")
    可輸入關鍵字字串進行搜尋，會回傳符合的搜尋結果list

以下是自訂函式介紹：
1.  gmap_search(api_key, place_id=None, keyword=None)
    必要輸入：api key；擇一輸入：place_id 或 keyword。會自動回傳gmap搜尋結果
    
2.  get_place_id(api_key: str, keyword: str)
    如果只有關鍵字，可以用來查詢place id（僅回傳搜尋結果的第一個項目）

3.  gmap_info(ori_name, api_key, place_id=None)
    輸入place id會自動搜尋並取得專題需要的欄位資訊並存成字典回傳

4.  get_place_dict(name=None, address=None, keyword=None,
    api_key_path=None, api_key=None, place_id=None)
    更加完整自動化的函式，幾乎可以適配任何輸入情形，並將結果存成字典回傳

"""


def get_key(api_key_path: str) -> str:
    """提供key檔案路徑，自動讀取內容並回傳"""
    with open(api_key_path, "r", encoding="UTF-8") as f:
        api_key = f.read()

    return api_key


def trans_unix_to_date(timestamp: datetime.fromtimestamp) -> str:
    """將UNIX電腦時間轉換為西元年月日"""
    # 將時間戳轉成 datetime 物件
    dt = datetime.fromtimestamp(timestamp)

    # 格式化成 "年-月-日"
    date_str = dt.strftime("%Y-%m-%d")

    return date_str


def newest_review_date(review_list: list) -> str:
    """提供gmaps回傳的評論列表，回傳列表中最新的評論時間"""
    for i in review_list:
        time = 0
        if i["time"] > 0:
            time = i["time"]
    date = trans_unix_to_date(time)

    return date


def gmap_search(api_key: str, place_id: Optional[str] = None, keyword: Optional[str] = None) -> dict:
    """
    簡單的gmap搜尋，根據提供place id或keyword，回傳相應的結果。
    回傳內容為未經處理的原始資訊。
    """

    gmaps = googlemaps.Client(key=api_key)

    if place_id is not None:
        search_result = gmaps.place(place_id=place_id, language="zh-TW")
        return search_result
    elif keyword is not None:
        search_result = gmaps.places(query=keyword, language="zh-TW")
        return search_result
    else:
        raise TypeError("請輸入 Place ID 或 keyword")


def get_place_id(api_key: str, keyword: str) -> str:
    """提供關鍵字（最好是店名+地址），透過gmap搜尋取得搜尋結果的第一個place_id"""
    gmaps = googlemaps.Client(key=api_key)
    search_result = gmaps.places(query=keyword, language="zh-TW")
    if len(search_result["results"]) != 0:
        place_id = search_result.get("results", {})[0]["place_id"]
        return place_id
    else:
        return None


def empty_object(ori_name: str, place_id: Optional[str] = None) -> dict:
    """當查無資料時，提供空的店家資訊dict"""
    return {
        "name": ori_name,
        "place_id": place_id,
        "business_status": None,
        "address": None,
        "phone": None,
        "opening_hours": None,
        "types": None,
        "rating": None,
        "rating_total": None,
        "longitude": None,
        "latitude": None,
        "map_url": None,
        "website": None,
        "newest_review": None,
    }


def place_id_na_or_not(place_id: str) -> bool:
    na_list = {None, "", "nan", "null", "na"}
    if place_id is None:
        return False
    if isinstance(place_id, float) and math.isnan(place_id):
        return False
    if isinstance(place_id, str) and place_id.strip().lower() in na_list:
        return False
    return True


def gmap_info(ori_name: str, api_key: str, place_id: Optional[str] = None) -> dict:
    """提供place_id，回傳名稱、營業狀態、營業時間、gmap評分、經緯度、gmap網址、最新評論日期"""

    if place_id_na_or_not(place_id):
        try:
            gmaps = googlemaps.Client(key=api_key)
            detail = gmaps.place(place_id=place_id, language="zh-TW")

        except Exception as e:
            print(f"發生錯誤：{e}回傳空物件")
            return empty_object(ori_name, place_id)

        name = detail.get("result", {}).get("name", None)
        business_status = detail.get("result", {}).get("business_status", None)

        if detail.get("result", {}).get("formatted_address"):
            address = detail.get("result", {}).get("formatted_address", None)
        elif detail.get("result", {}).get("adr_address"):
            address = BeautifulSoup(
                detail.get("result", {}).get(
                    "adr_address", None), "html.parser"
            ).text
        else:
            address = None

        phone = detail.get("result", {}).get("formatted_phone_number", None)
        phone = phone.replace(" ", "") if phone else None

        opening_hours = (
            detail.get("result", {}).get(
                "opening_hours", {}).get("weekday_text", None)
        )
        types = detail.get("result", {}).get("types", None)
        rating = detail.get("result", {}).get("rating", None)
        rating_total = detail.get("result", {}).get("user_ratings_total", None)
        longitude = (
            detail.get("result", {})
            .get("geometry", {})
            .get("location", {})
            .get("lng", None)
        )
        latitude = (
            detail.get("result", {})
            .get("geometry", {})
            .get("location", {})
            .get("lat", None)
        )
        map_url = detail.get("result", {}).get("url", None)
        website = detail.get("result", {}).get("website", None)
        review_list = detail.get("result", {}).get("reviews", None)

        if review_list is not None:
            newest_review = newest_review_date(review_list)
        else:
            newest_review = None

        place_info = {
            "name": name,
            "place_id": place_id,
            "business_status": business_status,
            "address": address,
            "phone": phone,
            "opening_hours": opening_hours,
            "types": types,
            "rating": rating,
            "rating_total": rating_total,
            "longitude": longitude,
            "latitude": latitude,
            "map_url": map_url,
            "website": website,
            "newest_review": newest_review,
        }
    else:
        place_info = empty_object(ori_name, place_id)

    return place_info


def get_place_dict(
    name: Optional[str] = None,
    address: Optional[str] = None,
    keyword: Optional[str] = None,
    api_key_path: Optional[str] = None,
    api_key: Optional[str] = None,
    place_id: Optional[str] = None,
) -> dict:
    """
    可輸入：
    1.（必填）"key"或"帶key的檔案路徑"。有key才能使用gmap搜尋服務。
    2.下列三種方式擇一：
        「名稱+地址」自動組合成關鍵字搜尋，並回傳第一個結果的地標資訊。
        「關鍵字」直接輸入關鍵字並搜尋，回傳第一個結果的地標資訊。
        「地標的place id」回傳唯一的地標資訊。
    """

    # 先確認key有輸入，有key才能使用服務
    if api_key_path is not None:
        api_key = get_key(api_key_path=api_key_path)
    elif api_key is None:
        raise TypeError("請輸入 API key 或 API key 路徑！")

    # 接著確認用哪種方式進行搜尋
    # 如果有place id可直接精準搜尋回傳資訊
    if place_id is not None:
        place_dict = gmap_info(
            ori_name=name, api_key=api_key, place_id=place_id)
        return place_dict

    # 若沒有place id，再確認是輸入關鍵字或是名字和地址
    if keyword is None:
        if name is None and address is None:
            raise TypeError("請擇一輸入 keyword/name+address")
        if name is None:
            name = ""
        if address is None:
            address = ""

        keyword = (name + " " + address).strip()

    place_id = get_place_id(api_key=api_key, keyword=keyword)
    place_dict = gmap_info(ori_name=name, api_key=api_key, place_id=place_id)
    return place_dict


def gmap_nearby_search(key: str, lat: float, lon: float, radius: int, keyword: str) -> list[dict]:
    """
    使用gmap地理搜尋功能：設定中心點（經緯度）並搜尋半徑內符合關鍵字的地標。
    需提供：api key、經度、緯度、搜尋半徑、關鍵字等資訊。

    注意：半徑搜尋每次提供單頁20筆、上限3頁的資料數，請設定適合的搜尋半徑。
    """

    # 使用gmap連線並搜尋，取得搜尋結果
    gmaps = googlemaps.Client(key=key)
    search_result = gmaps.places_nearby(
        location=(lat, lon), radius=radius, keyword=keyword
    )
    result = search_result.get("results", [])
    page = 1

    # 取出第一頁搜尋結果的名稱、place_id和地址，存成字典後加入list
    result_data = []
    for place in result:
        place_dict = {
            "name": place.get("name", None),
            "place_id": place.get("place_id", None),
            "buss_status": place.get("business_status", None),
            "geometry": place.get("geometry", {}).get("location", None)
        }
        result_data.append(place_dict)

    # 進入迴圈判斷是否有下一頁，如果沒有直接停止，若有則迴圈
    time.sleep(2.5)
    next_page = search_result.get("next_page_token", None)

    while next_page:
        page += 1
        max_tries = 3
        for tries in range(1, max_tries + 1):
            try:
                print(f"正在取得第{page}頁資訊...")
                time.sleep(2.5)
                next_page_search = gmaps.places_nearby(page_token=next_page)
                next_page_result = next_page_search.get("results", [])
                for place in next_page_result:
                    place_dict = {
                        "name": place.get("name", None),
                        "place_id": place.get("place_id", None),
                        "buss_status": place.get("business_status", None),
                        "geometry": place.get("geometry", {}).get("location", None)
                    }
                    result_data.append(place_dict)
                time.sleep(2.5)
                next_page = next_page_search.get("next_page_token", None)
                break

            except Exception as e:
                if tries >= max_tries:
                    print("已達最大嘗試次數")
                    next_page = None
                    break
                else:
                    print(f"第{tries}次嘗試失敗：{e}\n等待3秒後重試...")
                    time.sleep(3)

    return result_data
