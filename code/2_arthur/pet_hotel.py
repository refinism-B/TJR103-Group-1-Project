import requests
import pandas as pd
from bs4 import BeautifulSoup

url = "https://www.pet.gov.tw/Web/BusinessList.aspx"
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36 Edg/141.0.0.0"
}
data = {
    "action": "GetBusinessList2",
    "_BusinessList": {
        "Countycode": "",
        "PBLicense": "",
        "BI": "C",
        "BussName": "",
        "EvaluationLevel": "",
        "KeyWord": "旅",
        "pageSize": 500,
        "currentPage": "1",
        "IsActive": "1",
    },
}


response = requests.post(url, data, headers, verify=False)
soup = BeautifulSoup(response.text, "html.parser")

city = soup.select("tbody td[data-th='所屬縣市']")
print(city)
