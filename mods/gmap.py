from datetime import datetime

import googlemaps
from bs4 import BeautifulSoup

"""
這個模組主要用於使用google map API爬取資料

- 基本用法：
    1.  先取得api key
    2.  使用gmaps = googlemaps.Client(key=api_key)建立連線
    3.  使用gmaps.places(query=key_word, language='zh-TW')
        以關鍵字方式搜尋google map，並回傳搜尋結果列表（每一
        個結果的資訊會以字典的形式組成）
    4.  使用gmaps.place(place_id=place_id, language='zh-TW')
        以place id的方式搜尋google map地標（為精確搜尋，只會有
        一個結果）資訊也是由字典形式組成
    5.  字典中除了某些必要欄位一定會有，某些欄位是不一定會有的，
        所以建議用.get()的方式取值，若沒有該欄位則會回傳None
    6.  需要安裝第三方套件「googlemaps」但應該都已經在poetry中
        了，如果沒有可以再執行一次「poetry install」檢查看看有
        沒有漏裝的套件

以下是這個模組自訂的函式，如有需要可以使用

- get_key(api_key_path)：  
    若金鑰以 txt 檔案的形式存檔，將 txt 檔路徑輸入，
    便會自動 return key 值。

- get_place_id(api_key, key_word)：  
    將金鑰帶入變數 1，將關鍵字字串帶入變數 2，會自
    回傳搜尋的第一個結果的 place id。

- gmap_info(ori_name, api_key, place_id)：  
    將金鑰帶入變數 2，place id 帶入變數 3，會自動
    抓取地點資訊（主要是專題需要的資訊，詳細欄位
    可看程式碼）。變數 1 是自行輸入的名稱字串，以防
    搜尋不到結果時可以回傳只有名稱、其他為 None 的
    字典。

- get_place_dict(api_key_path, name: str, address: str)：  
    上述函式的組合版，將 api key 的檔案路徑帶入變
    數 1，地點名稱帶入變數 2，地址帶入變數 3，會自
    動回傳該地資訊的字典（專題所需的欄位）。

- newest_review_date(review_list: list)：  
    輸入評論的 list（透過 gmap 取得的 review 內容），
    會找出並回傳最新的評論日期。

- trans_unix_to_date(timestamp)：  
    將 unix 電腦時間轉換成現在的年月日。搭配上一個
    函式，因 gmap 回傳的時間為 unix 時間，需要轉換。

原本只是寫給自己方便使用，所以沒有做太多防呆，如果有
個別需求建議還是使用套件本身的函式 gmaps.places()
或 gmaps.place() 會比較靈活。

API 訪問次數為每月限額（實際次數有點混亂但應該夠我們使
用），所以建議在測試階段先以部分或少量資料做練習或測試，
等開發的完成後，再使用完整資料。另外建議也可以先存取每
個地點的 place id，之後就可以使用 place id 精確搜尋，
免除找 place id 的步驟。
"""


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
    date_str = dt.strftime("%Y-%m-%d")

    return date_str


def newest_review_date(review_list: list):
    """提供gmaps回傳的評論列表，回傳列表中最新的評論時間"""
    for i in review_list:
        time = 0
        if i["time"] > 0:
            time = i["time"]
    date = trans_unix_to_date(time)

    return date


def gmap_search(api_key, place_id=None, keyword=None):
    gmaps = googlemaps.Client(key=api_key)

    if place_id is not None:
        search_result = gmaps.place(place_id=place_id, language="zh-TW")
        return search_result
    elif keyword is not None:
        search_result = gmaps.places(query=keyword, language="zh-TW")
        return search_result
    else:
        raise TypeError("請輸入 Place ID 或 keyword")


def get_place_id(api_key: str, keyword: str):
    """提供關鍵字（最好是店名+地址），透過gmap搜尋取得搜尋結果的第一個place_id"""
    gmaps = googlemaps.Client(key=api_key)
    search_result = gmaps.places(query=keyword, language="zh-TW")
    if len(search_result["results"]) != 0:
        place_id = search_result.get("results", {})[0]["place_id"]
        return place_id
    else:
        return None


def gmap_info(ori_name, api_key, place_id=None):
    """提供place_id，回傳名稱、營業狀態、營業時間、gmap評分、經緯度、gmap網址、最新評論日期"""
    if place_id is not None:
        gmaps = googlemaps.Client(key=api_key)
        detail = gmaps.place(place_id=place_id, language="zh-TW")
        name = detail.get("result", {}).get("name", None)
        business_status = detail.get("result", {}).get("business_status", None)

        if detail["result"]["formatted_address"]:
            address = detail.get("result", {}).get("formatted_address", None)
        elif detail["result"]["adr_address"]:
            address = BeautifulSoup(
                detail.get("result", {}).get("adr_address", None), "html.parser"
            ).text
        else:
            address = None

        phone = detail.get("result", {}).get("formatted_phone_number", None)
        if phone is not None:
            phone = phone.replace(" ", "")

        opening_hours = (
            detail.get("result", {}).get("opening_hours", {}).get("weekday_text", None)
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
        place_info = {
            "name": ori_name,
            "place_id": None,
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

    return place_info


def get_place_dict(
    name=None,
    address=None,
    keyword=None,
    api_key_path=None,
    api_key=None,
    place_id=None,
):
    """輸入：key or key.txt 檔案路徑；名稱和地址or關鍵字or place_id。
    會自動回傳地標資訊的字典"""

    # 先確認key有輸入，有key才能使用服務
    if api_key_path is not None:
        api_key = get_key(api_key_path=api_key_path)
    elif api_key is None:
        raise TypeError("請輸入 API key 或 API key 路徑！")

    # 接著確認用哪種方式進行搜尋
    ## 如果有place id可直接精準搜尋回傳資訊
    if place_id is not None:
        place_dict = gmap_info(ori_name=name, api_key=api_key, place_id=place_id)
        return place_dict

    ## 若沒有place id，再確認是輸入關鍵字或是名字和地址
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


def gmap_nearby_search(key, lat, lon, radius, keyword):
    """提供api_key、經緯度、搜尋半徑和關鍵字，回傳搜尋結果的列表，資訊包括名稱、place_id和營業狀態"""
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
