這個模組主要用於使用 google map API 爬取資料

- 基本用法：

  1.  先取得 api key

  2.  使用 **gmaps = googlemaps.Client(key=api_key)** 建立連線

  3.  使用 **gmaps.places(query=key_word, language='zh-TW')**
      以關鍵字方式搜尋 google map，並回傳搜尋結果列表（每一
      個結果的資訊會以字典的形式組成）

  4.  使用 **gmaps.place(place_id=place_id, language='zh-TW')**
      以 place id 的方式搜尋 google map 地標（為精確搜尋，只會有
      一個結果）資訊也是由字典形式組成

  5.  字典中除了某些必要欄位一定會有，某些欄位是不一定會有的，
      所以建議用.get()的方式取值，若沒有該欄位則會回傳 None

  6.  需要安裝第三方套件「googlemaps」但應該都已經在 poetry 中
      了，如果沒有可以再執行一次「poetry install」檢查看看有
      沒有漏裝的套件

以下是這個模組自訂的函式，如有需要可以使用

- **get_key(api_key_path)**：  
   若金鑰以 txt 檔案的形式存檔，將 txt 檔路徑輸入，
  便會自動 return key 值。

- **get_place_id(api_key, key_word)**：  
   將金鑰帶入變數 1，將關鍵字字串帶入變數 2，會自
  回傳搜尋的第一個結果的 place id。

- **gmap_info(ori_name, api_key, place_id)**：  
   將金鑰帶入變數 2，place id 帶入變數 3，會自動
  抓取地點資訊（主要是專題需要的資訊，詳細欄位
  可看程式碼）。變數 1 是自行輸入的名稱字串，以防
  搜尋不到結果時可以回傳只有名稱、其他為 None 的
  字典。

- **get_place_dict(api_key_path, name: str, address: str)**：  
   上述函式的組合版，將 api key 的檔案路徑帶入變
  數 1，地點名稱帶入變數 2，地址帶入變數 3，會自
  動回傳該地資訊的字典（專題所需的欄位）。

- **newest_review_date(review_list: list)**：  
   輸入評論的 list（透過 gmap 取得的 review 內容），
  會找出並回傳最新的評論日期。

- **trans_unix_to_date(timestamp)**：  
   將 unix 電腦時間轉換成現在的年月日。搭配上一個
  函式，因 gmap 回傳的時間為 unix 時間，需要轉換。

原本只是寫給自己方便使用，所以沒有做太多防呆，如果有
個別需求建議還是使用套件本身的函式 **gmaps.places()**
或 **gmaps.place()** 會比較靈活。

API 訪問次數為每月限額（實際次數有點混亂但應該夠我們使
用），所以建議在測試階段先以部分或少量資料做練習或測試，
等開發的完成後，再使用完整資料。另外建議也可以先存取每
個地點的 place id，之後就可以使用 place id 精確搜尋，
免除找 place id 的步驟。
