import requests
from bs4 import BeautifulSoup
import pandas as pd 
import re 
from urllib.parse import quote

# 來源網站
Baseurl = "https://taiwan.petboo.co"
# 資料類別是餐廳
pettype = "fnb"
# 每次請求間隔0.8-1.6秒
delay_range = (0.8, 1.6)
input_csv = "city_districts.csv"
output_csv = "petboo_fnb_retest.csv"  

useragent = "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Mobile Safari/537.36"
myheaders = {"User-Agent" : useragent}
res = requests.get(Baseurl, headers=myheaders)
print(res.text)


# 取html的內容 
def get_html(url):
    # 取網址的html原始碼
    # 成功會回傳r.text就是HTML文字，失敗就回傳空字串
    try:
        # 發送requsets,最多等待20秒
        r = requests.get(url, headers=myheaders, timeout=20)
        # 狀態碼必須是200，內容型態包含text/html
        if r.status_code == 200 and "text/html" in r.headers.get("Content-Type", "").lower():
            return r.text
    except requests.RequestException:
        # 若出現問題，直接略過
        pass
    return ""

# 讀城市/行政區名單並把所有NaN換成空字串
targets = pd.read_csv(input_csv, dtype=str).fillna("")

# 準備一個list裝
rows = []

# 處理每個城市跟區
for t, row in targets.iterrows():
    city = row["city"]
    district = row["district"]
    print(f"開始爬：{city} {district}")

    # 抓前5頁
    for page in range(1, 6):
        # 組網址：/places/<pettype>/<city>/<district>/<page>
        list_url = f"{Baseurl}/places/{pettype}/{quote(city)}/{quote(district)}/{page}"
        print("列表頁：", list_url)

        # 抓列表頁html
        list_html = get_html(list_url)
        if not list_html:
            # 抓不到內容 跳出頁面迴圈
            print("無內容")
            break

        # 用BeautifulSoup解析列表頁
        soup_list = BeautifulSoup(list_html, "html.parser")

        # 從列表頁把所有/place/連結找出來
        links = []
        seen = set()
        for a in soup_list.select('a[href*="/place/"]'):
            href = (a.get("href") or "").strip()
            # 過濾假連結(#,javascript)
            if not href or href.startswith("#") or href.startswith("javascript:"):
                continue

            # 手動補網址
            if href.startswith("http"):
                full = href
            elif href.startswith("/"):
                full = Baseurl + href
            else:
                full = Baseurl + "/" + href

            # 避免同連結重複處理
            if full not in seen:
                seen.add(full)
                links.append(full)

        print(f"本頁找到 {len(links)} 個店家")

        if not links:
            # 沒有店跳出
            break

        # 進入每一家店的詳情頁
        for detail_url in links:
            dhtml = get_html(detail_url)
            
            if not dhtml:
                # 這家抓不到就跳過
                continue

            soup = BeautifulSoup(dhtml, "html.parser")

            # 名稱在 <h1>
            name = ""
            h1 = soup.select_one("h1") 
            if h1:
                name = h1.get_text(" ", strip=True)

            # 地址取 Google Maps 連結的可見文字
            address = ""
            a_map = soup.select_one("a[href*='maps']")
            if a_map:
                address = a_map.get_text(" ", strip=True)

            # 電話tel: 連結
            phone = ""
            tel = soup.select_one('a[href^="tel:"]')
            if tel:
                phone = (tel.get("href") or "")[4:].strip()  # 去掉 'tel:' 前綴

            # 評分 評論數量 寵物規則
            full_text = soup.get_text(" ", strip=True)
            
            # 會抓出兩組數字：
            # group(1) = 評分
            # group(2) = 評分人數
            rating_re = re.compile(r"(\d+(?:\.\d+)?)\s*[\(（]\s*([0-9,]+)\s*[\)）]")

            # 常見寵物規則關鍵字（你可自行增刪）
            pet_rulewords = [
                "可落地", "需牽繩", "須提籃", "抱在腿上",
                "寵物友善", "需推車", "需嘴套", "需尿布", "不可落地"]
            
            m = rating_re.search(full_text.replace(",", "").replace("，", ""))
            if m:
                rating = m.group(1)        # 評分
                rating_count = m.group(2)  # 評論數
            else:
                rating = ""
                rating_count = ""

            # 關鍵字：有提到就收集
            pet_rules = "、".join([w for w in pet_rulewords if w in full_text])

            # 存到 rows 每家店一筆
            rows.append({
                "city": city,
                "district": district,
                "name": name,
                "address": address,
                "phone": phone,
                "rating": rating,
                "rating_count": rating_count,
                "pet_rules": pet_rules,
                "url": detail_url
            })

            print(f"{name or '(無名稱)'} | {rating or '-'}分 | {pet_rules or '-'}")

# rows > dataFrame >存成CSV
df = pd.DataFrame(rows)
df.to_csv(output_csv, index=False, encoding="utf-8")
print(f"共{len(df)} 筆,存到{output_csv}")
