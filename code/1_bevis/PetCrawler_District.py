import requests
from bs4 import BeautifulSoup
from datetime import date, datetime, timedelta
import json
import pandas as pd
import time
import os
from pathlib import Path


# 自訂函式
def get_city_list(url, headers):
    """用於取得各縣市之代碼，並轉換成list方便迴圈"""
    fmt = "%Y/%m/%d"
    start = date.today() - timedelta(days=1)
    start_date = start
    end_date = (start_date + timedelta(days=1)).strftime(fmt)
    start_date = start_date.strftime(fmt)

    data = {
        "Method":"O302_2",
        "Param":json.dumps({
            "SDATE":start_date,
            "EDATE":end_date,
            "Animal":"0",
        })}

    res = requests.post(url, headers=headers, data=data)
    res.encoding = "utf-8-sig"

    data_orig = json.loads(res.text)
    data_str = data_orig.get("Message", "[]")
    data_json = json.loads(data_str)

    df_id_list = pd.DataFrame(data_json)
    country_list = list(df_id_list["AreaID"])

    return country_list



"""
爬蟲程式的結構為：
「根據縣市＞根據日期＞根據犬/貓類別」進行迴圈。

資料檔案是根據「縣市」存檔，
故這樣的順序只在第一層迴圈時讀取該縣市的檔案，
直到該縣市資料更新完畢才會再讀取下一個縣市檔案，
避免頻繁切換/讀取檔案的情況產生。

存檔則是在每一次完成犬/貓資料爬取時就存檔，
避免爬了一長段資料後，程式因意外中止，
但由於爬取過程都沒有存檔，
導致爬取的資料完全丟失，浪費時間也浪費資源。
"""


# 設定訪問網址及headers
url = "https://www.pet.gov.tw/Handler/PostData.ashx"

headers = {"user-agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36"}


# 主要爬蟲程式
def main():
    # 先取得縣市代碼list
    city_list = get_city_list(url, headers)

    #　取得當前日期
    today = date.today()

    # 區分犬貓代號
    animal = ["0", "1"]

    # 代碼對應的縣市名，用於存檔檔名
    city_dict = {
        "A":"NewTaipei",
        "V":"Taipei",
        "S":"Taichung",
        "U":"Tainan",
        "W":"Kaohsiung",
        "C":"Taoyuan",
        "B":"Yilan",
        "D":"Hsinchu",
        "E":"Miaoli",
        "G":"Changhua",
        "H":"Nantou",
        "I":"Yunlin",
        "J":"Chiayi",
        "M":"Pingtung",
        "N":"Taitung",
        "O":"Hualien",
        "P":"Penghu",
        "Q":"Keelung",
        "R":"HsinchuCity",
        "T":"ChiayiCity",
        "Y":"Kinmen",
        "X":"Lianjiang"
    }

    # 開始按照縣市代碼list進行迴圈
    for dist in city_list:

        # 根據縣市設定檔案路徑
        file_path = f"C:/Users/add41/Documents/Data_Engineer/Project/Pet-Crawler/PetData/district/{city_dict[dist]}.csv"
        print(f"開始搜尋{city_dict[dist]}鄉鎮市區資料...")

        # 先判斷是否存在既有檔案，若有則讀入，若無則建立新的DF
        if os.path.exists(file_path):
            df_main = pd.read_csv(file_path)
            df_main.index = pd.to_datetime(df_main.index)
        else:
            columns = [
            "AreaID",
            "AreaName",
            "fld01",
            "fld02",
            "fld03",
            "fld05",
            "fld06",
            "fld04",
            "fld08",
            "fld07",
            "fld10",
            "animal"
            ]

            df_main = pd.DataFrame(columns=columns)


        # 判斷是否已有資料存在，若無則從頭爬取，若有則從最後一筆資料日期開始接續
        if len(df_main.index) == 0:
            start = datetime.strptime("2025/10/08", "%Y/%m/%d").date()
        else:
            last_date = df_main.index[-1]
            start = last_date + timedelta(days=1)
            start = start.date()
        

        # 若起始日期早於當下日期，則開始迴圈更新資料
        while start < today:
            fmt = "%Y/%m/%d"
            start_date = start
            end_date = (start_date + timedelta(days=1)).strftime(fmt)
            start_date = start_date.strftime(fmt)

            # 根據兩種寵物類別分別迴圈爬取資料（因需帶入不同資料POST）
            for ani in animal:
                data = {
                    "Method":"O302C_2",
                    "Param":json.dumps({
                        "SDATE":start_date,
                        "EDATE":end_date,
                        "Animal":ani,
                        "CountyID":dist
                    })}

                # 設定最大嘗試次數3次，若是因間隔過短或許可在多次嘗試後成功
                max_tries = 3
                for tries in range(1, max_tries+1):
                    try:
                        res = requests.post(url, headers=headers, data=data)
                        res.encoding = "utf-8-sig"

                        # 因資料是以json格式儲存和回傳，故需json解碼
                        data_orig = json.loads(res.text)
                        data_str = data_orig.get("Message", "[]")
                        data_json = json.loads(data_str)

                        # 儲存成DF後新增日期、寵物類別、縣市欄位
                        df = pd.DataFrame(data_json)
                        df["animal"] = ani
                        df["date"] = start_date
                        df["city"] = dist

                        # 將索引改為日期，並將日期欄位去除
                        df.index = pd.to_datetime(df["date"])
                        df.drop(columns="date", inplace=True)

                        # 將爬取的資料與原始資料結合並存檔
                        # 避免程式意外中止時，會因完全沒有存檔而丟失紀錄，浪費時間及資源
                        df_main = pd.concat([df_main, df])
                        df_main.to_csv(file_path)
                        time.sleep(7)
                        break
                    
                    except Exception as e:
                        if tries >= max_tries:
                            print("已達最大嘗試次數，跳過該日")
                            break
                        else:
                            print(f"第{tries}次嘗試錯誤：{e}\n等待{tries*5}秒後再次嘗試...")
                            time.sleep(tries*5)
                            continue

            # 每完成一日犬貓資料更新，就印出訊息告知，並將起始日+1重複迴圈
            print(f"已更新{city_dict[dist]} {start_date}犬貓資料")
            start += timedelta(days=1)

        # 當完成一個縣市資料更新，就印出訊息告知
        print(f"已完成更新{city_dict[dist]}資料至{today}！")

    print("已完成所有資料更新！")




if __name__ == "__main__":
    main()