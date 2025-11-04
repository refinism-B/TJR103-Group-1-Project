import sys, os, requests, pandas as pd
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))
sys.path.append(ROOT_DIR)

from mods import gmap as gm
from dotenv import load_dotenv
from config import API_KEY, API_LINK

def get_api_json(url):
    res = requests.get(url, verify=False, timeout=15)
    res.raise_for_status()
    return res.json()

def extract_shelter_data():
    """從農業部 API 取得收容所基本資料"""
    data = get_api_json(API_LINK)
    df = pd.DataFrame(data)
    df = df[["ShelterName", "CityName", "Address", "Phone"]].rename(columns={
        "ShelterName": "收容所名稱",
        "CityName": "縣市",
        "Address": "地址",
        "Phone": "電話",
    })
    return df
