import json
import os
import time
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv
import pymysql
import GCS_mod as GCS


# 自訂函式
def get_city_list(url, headers):
    """用於取得各縣市之代碼，並轉換成list方便迴圈"""
    fmt = "%Y/%m/%d"
    start = date.today() - timedelta(days=1)
    start_date = start
    end_date = (start_date + timedelta(days=1)).strftime(fmt)
    start_date = start_date.strftime(fmt)

    data = {
        "Method": "O302_2",
        "Param": json.dumps(
            {
                "SDATE": start_date,
                "EDATE": end_date,
                "Animal": "0",
            }
        ),
    }

    res = requests.post(url, headers=headers, data=data)
    res.encoding = "utf-8-sig"

    data_orig = json.loads(res.text)
    data_str = data_orig.get("Message", "[]")
    data_json = json.loads(data_str)

    df_id_list = pd.DataFrame(data_json)
    country_list = list(df_id_list["AreaID"])

    return country_list


def get_city_code():
    return {
        "A": "NewTaipei",
        "V": "Taipei",
        "S": "Taichung",
        "U": "Tainan",
        "W": "Kaohsiung",
        "C": "Taoyuan",
        "B": "Yilan",
        "D": "Hsinchu",
        "E": "Miaoli",
        "G": "Changhua",
        "H": "Nantou",
        "I": "Yunlin",
        "J": "Chiayi",
        "M": "Pingtung",
        "N": "Taitung",
        "O": "Hualien",
        "P": "Penghu",
        "Q": "Keelung",
        "R": "HsinchuCity",
        "T": "ChiayiCity",
        "Y": "Kinmen",
        "X": "Lianjiang",
    }


def get_col():
    return [
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
        "animal",
        "date",
        "city",
        "update_date"
    ]


def set_start_date(df_main):
    fmt = "%Y/%m/%d"
    if len(df_main.index) == 0:
        start = datetime.strptime("2020/01/01", fmt).date()
    else:
        df_main["date"] = pd.to_datetime(df_main["date"], format=fmt)
        last_date = df_main["date"].iloc[-1]
        start = last_date + timedelta(days=1)
        start = start.date()

    return start


def get_start_end_date(start):
    fmt = "%Y/%m/%d"
    start_date = start
    end_date = (start_date + timedelta(days=1)).strftime(fmt)
    start_date = start_date.strftime(fmt)

    return start_date, end_date


def post_data(start_date, end_date, ani, dist):
    return {
        "Method": "O302C_2",
        "Param": json.dumps(
            {
                "SDATE": start_date,
                "EDATE": end_date,
                "Animal": ani,
                "CountyID": dist,
            }),
    }


def post_requests(url, headers, data):
    res = requests.post(url=url, headers=headers, data=data)
    res.encoding = "utf-8-sig"

    # 因資料是以json格式儲存和回傳，故需json解碼
    data_orig = json.loads(res.text)
    data_str = data_orig.get("Message", "[]")
    data_json = json.loads(data_str)

    return data_json


def stop_try(tries, max_tries):
    if tries >= max_tries:
        return True
    else:
        return False


def get_formal_cols():
    return [
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


def get_df_loc():
    load_dotenv()

    username = os.getenv("MYSQL_USERNAME")
    password = os.getenv("MYSQL_PASSWORD")
    target_ip = os.getenv("MYSQL_IP")
    target_port = int(os.getenv("MYSQL_PORTT"))
    db_name = os.getenv("MYSQL_DB_NAME")

    conn = pymysql.connect(
        host=target_ip,
        port=target_port,
        user=username,
        password=password,
        database=db_name,
        charset='utf8mb4'
    )

    sql = "SELECT * FROM location"
    df = pd.read_sql(sql, conn)

    return df


def get_gcs_setting(folder_date: str, local_path: str) -> dict:
    return {
        "bucket_name": "tjr103-1-project-bucket",
        "destination": f"complete/registration/dt={folder_date}/registration.csv",
        "source_file_name": f"{local_path}"
    }


# 設定訪問網址及headers
url = "https://www.pet.gov.tw/Handler/PostData.ashx"

headers = {
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36"
}

city_val_dict = {
    "A": "新北市",
    "V": "臺北市",
    "C": "桃園市",
    "S": "臺中市",
    "U": "臺南市",
    "W": "高雄市"
}

# 先取得縣市代碼list
# city_list = get_city_list(url, headers) # 這行用於取得全台縣市代號
city_list = ["A", "V", "C", "S", "U", "W"]

# 將地區表讀入後續使用
df_loc = get_df_loc()
df_loc = df_loc[["loc_id", "city", "district"]]

# 取得當前日期
end_str = "2022/01/01"
end_date = datetime.strptime(end_str, "%Y/%m/%d").date()
start_str = "2020/01/01"
start_date = datetime.strptime(start_str, "%Y/%m/%d").date()

# 區分犬貓代號
animal = ["0", "1"]

# 代碼對應的縣市名，用於存檔檔名
city_dict = get_city_code()

while start_date < end_date:
    start_data, end_data = get_start_end_date(start=start_date)
    print(f"開始抓取{start_data}資料...")
    cols = get_col()
    df_main = pd.DataFrame(columns=cols)

    for dist in city_list:
        for ani in animal:
            data = post_data(start_data, end_data, ani, dist)

            # 設定最大嘗試次數3次，若是因間隔過短或許可在多次嘗試後成功
            max_tries = 3
            for tries in range(1, max_tries + 1):
                try:
                    data_json = post_requests(
                        url=url, headers=headers, data=data)

                    # 儲存成DF後新增日期、寵物類別、縣市欄位
                    df = pd.DataFrame(data_json)
                    df["animal"] = ani
                    df["date"] = start_date.strftime("%Y/%m/%d")
                    df["city"] = dist
                    df["update_date"] = today_date.strftime("%Y/%m/%d")

                    # 將爬取的資料與原始資料結合並存檔
                    # 避免程式意外中止時，會因完全沒有存檔而丟失紀錄，浪費時間及資源
                    df_main = pd.concat([df_main, df], ignore_index=True)

                    start_file_str = start_date.strftime("%Y-%m-%d")
                    folder = Path(
                        f"C:/Users/add41/Documents/Data_Engineer/Project/TJR103-Group-1-Project/data/raw/pet_registry/dt={start_file_str}")
                    folder.mkdir(parents=True, exist_ok=True)
                    file_name = "registration_count.csv"
                    path = folder / file_name

                    time.sleep(7)
                    break

                except Exception as e:
                    stop = stop_try(tries, max_tries)
                    if stop:
                        print("已達最大嘗試次數，跳過該日")
                        break
                    else:
                        print(
                            f"第{tries}次嘗試錯誤：{e}\n等待{tries*5}秒後再次嘗試..."
                        )
                        time.sleep(tries * 5)
                        continue

    formal_cols = get_formal_cols()

    df_main.columns = formal_cols

    df_main = df_main.drop(columns=["area_id", "登記單位數", "轉讓數", "變更數",
                                    "絕育數", "絕育除戶數", "免絕育數", "免絕育除戶數"])

    df_main["district"] = df_main["district"].apply(lambda x: x[3:])
    df_main["city"] = df_main["city"].map(city_val_dict)

    df_main = df_main.merge(df_loc, how="left", on=["city", "district"])
    df_main = df_main.drop(columns=["city", "district"])

    new_col = ["loc_id", "date", "animal",
               "regis_count", "removal_count", "update_date"]
    df_main = df_main[new_col]

    start_file_str = start_date.strftime("%Y-%m-%d")
    folder = Path(
        f"C:/Users/add41/Documents/Data_Engineer/Project/TJR103-Group-1-Project/data/raw/pet_registry/dt={start_file_str}")
    folder.mkdir(parents=True, exist_ok=True)
    file_name = "registration_count.csv"
    path = folder / file_name

    df_main.to_csv(path, index=False, encoding="utf-8-sig")

    gcs_setting = get_gcs_setting(folder_date=start_file_str, local_path=path)

    GCS.L_upload_to_gcs(gcs_setting=gcs_setting)

    start_date += timedelta(days=1)

print("所有資料已儲存完畢！")
