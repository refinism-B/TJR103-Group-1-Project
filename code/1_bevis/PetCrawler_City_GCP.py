import requests
from bs4 import BeautifulSoup
from datetime import date, datetime, timedelta
import json
import pandas as pd
import time
from pathlib import Path
from google.cloud import storage


# 設定儲存檔案路徑
file_path = "/home/add412/project/pet_crawler/regis_data/city_data.csv"

file = Path(file_path)

def main():
    # 如已有資料存在，則讀入；若沒有資料則直接新建DF
    if file.exists():
        main_df = pd.read_csv(file_path, index_col=0)
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
        main_df = pd.DataFrame(columns=columns)


    # 定義日期，並判斷是否有既存資料。
    # 如果沒有資料（df長度為0）則從設定的日期開始
    # 如果有既存資料則從最後一筆日期的隔天開始
    today = date.today()

    if len(main_df.index) == 0:
        start = datetime.strptime("2000/01/01", "%Y/%m/%d").date()
    else:
        last_date = datetime.strptime(main_df.index[-1], "%Y/%m/%d").date()
        start = last_date + timedelta(days=1)


    # 當起始日早於今天日期，則啟動爬蟲，直到資料更新至昨天為止
    while start < today:
        try:
            # 設定訪問網址和headers
            url = "https://www.pet.gov.tw/Handler/PostData.ashx"

            headers = {"user-agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36"}

            # 定義查詢的起始和結束日期（每次以一天為單位爬取）
            fmt = "%Y/%m/%d"
            start_date = start
            end_date = (start_date + timedelta(days=1)).strftime(fmt)
            start_date = start_date.strftime(fmt)

            # POST請求的日期資料格式
            # {"SDATE":"2025/09/01","EDATE":"2025/09/30","Animal":"0"}

            # 因有犬貓兩種類別，故需要分兩次爬取
            animal = ["0", "1"]
            ani_dict = {
                "0":"狗",
                "1":"貓"
                }
            
            for ani in animal:
                data = {
                    "Method":"O302_2",
                    "Param":json.dumps({
                        "SDATE":start_date,
                        "EDATE":end_date,
                        "Animal":ani
                    })}
                
                # 設定最大嘗試次數3次，若只是等候時間過短，還有機會成功
                max_tries = 3
                for tries in range(1, max_tries+1):
                    try:
                        print(f"開始查詢{ani_dict[ani]}的{start_date}到{end_date}的資料")
                        res = requests.post(url, headers=headers, data=data)
                        res.encoding = "utf-8-sig"

                        # 資料以json格式儲存及回傳，故需解碼
                        data_orig = json.loads(res.text)
                        data_str = data_orig.get("Message", "[]")
                        data_json = json.loads(data_str)
                        df = pd.DataFrame(data_json)

                        # 新增資料日期及動物種類的欄位（原本沒有），並將多餘欄位drop
                        df["date"] = start_date
                        df["animal"] = ani
                        df = df.drop(columns="QueryDT")

                        # 將日期設定為索引，並將多餘欄位去除
                        df.index = pd.to_datetime(df["date"])
                        df.drop(columns="date", inplace=True)

                        # 將爬取的資料與原始資料結合並存檔
                        main_df = pd.concat([main_df, df])

                        # 每日檔案爬取完畢後就進行存檔
                        # 若程式因錯誤中止，至少不會因為全部沒有存檔而丟失紀錄、浪費時間資源
                        main_df.to_csv(file_path)

                        print(f"已儲存{ani_dict[ani]}{start_date}的資料")


                        bucket_name = "pet-crawler"
                        destination_file = "pet-regis-data/pet_city_data.csv"
                        credentials_path = "/home/add412/tool/tactile-pulsar-473901-a1-4763fa15e78b.json"

                        client = storage.Client.from_service_account_json(credentials_path)
                        bucket = client.bucket(bucket_name)
                        blob = bucket.blob(destination_file)

                        blob.upload_from_filename(file_path)

                        time.sleep(7)
                        break

                    except Exception as e:
                        if tries >= max_tries:
                            print("已達最大嘗試次數")
                            break
                        else:
                            print(f"第{tries}次嘗試錯誤: {e}\n等待{tries * 5}秒後再次嘗試")
                            time.sleep(tries * 5)
                            continue
            
            # 完成一日的資料爬取後就將起始日期+1，並再次迴圈
            start += timedelta(days=1)

        except Exception as e:
            print(e)
    
    print("已將資料更新至最新！")


if __name__ == "__main__":
    main()