這個模組主要用於 google map（以下簡稱 gmap）套件爬取資料。

gmap 套件基本用法：

1.  _gmaps = googlemaps.Client(key=api_key)_  
    帶入金鑰建立連線。
2.  _search_result = gmaps.place(place_id=place_id, language="zh-TW")_  
    place id 為每個 gmap 地標都有的唯一代碼，輸入 place id 即可取得該地標資訊。
3.  _search_result = gmaps.places(query=keyword, language="zh-TW")_  
    可輸入關鍵字字串進行搜尋，會回傳符合的搜尋結果 list

以下是自訂函式介紹：

1.  _gmap_search(api_key, place_id=None, keyword=None)_  
    必要輸入：api key；擇一輸入：place_id 或 keyword。會自動回傳 gmap 搜尋結果
2.  _get_place_id(api_key: str, keyword: str)_  
    如果只有關鍵字，可以用來查詢 place id（僅回傳搜尋結果的第一個項目）

3.  _gmap_info(ori_name, api_key, place_id=None)_  
    輸入 place id 會自動搜尋並取得專題需要的欄位資訊並存成字典回傳

4.  _get_place_dict(name=None, address=None, keyword=None,_  
    _api_key_path=None, api_key=None, place_id=None)_  
    更加完整自動化的函式，幾乎可以適配任何輸入情形，並將結果存成字典回傳
