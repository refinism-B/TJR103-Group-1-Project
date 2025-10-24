這個模組主要用於google map（以下簡稱gmap）套件爬取資料。

gmap套件基本用法：
1.  *gmaps = googlemaps.Client(key=api_key)*  
    帶入金鑰建立連線。
    
2.  *search_result = gmaps.place(place_id=place_id, language="zh-TW")*  
    place id為每個gmap地標都有的唯一代碼，輸入place id即可取得該地標資訊。
    
3.  *search_result = gmaps.places(query=keyword, language="zh-TW")*  
    可輸入關鍵字字串進行搜尋，會回傳符合的搜尋結果list

以下是自訂函式介紹：
1.  *gmap_search(api_key, place_id=None, keyword=None)*  
    必要輸入：api key；擇一輸入：place_id 或 keyword。會自動回傳gmap搜尋結果
    
2.  *get_place_id(api_key: str, keyword: str)*  
    如果只有關鍵字，可以用來查詢place id（僅回傳搜尋結果的第一個項目）

3.  *gmap_info(ori_name, api_key, place_id=None)*  
    輸入place id會自動搜尋並取得專題需要的欄位資訊並存成字典回傳

4.  *get_place_dict(name=None, address=None, keyword=None,*  
    *api_key_path=None, api_key=None, place_id=None)*  
    更加完整自動化的函式，幾乎可以適配任何輸入情形，並將結果存成字典回傳