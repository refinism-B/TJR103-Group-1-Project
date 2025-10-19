import pandas as pd
from mods import get_var_data as gvd
from mods import store_to_csv as stc

# 設定URL和headers
URL = "https://www.dogcatstar.com/blog/24hour-animal-hospital/?srsltid=AfmBOorT4sCf-7J95E1GuTg29XR3w-xYXl2qm-PZL1eOjPJ0k3ndzEem"
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36 Edg/141.0.0.0"
}
raw_path = "data/raw/hospital_24_hr_data.csv"
processed_path = "data/processed/hospital_24_hr_data_ETL.csv"


def main():
    soup = gvd.get_the_html(url=URL, headers=headers)

    # 所有的資訊都在p tag裡面
    p_tags = soup.select("p")
    p_texts = [p.get_text(strip=True) for p in p_tags]

    # 只擷取符合醫院的elements
    p_texts = p_texts[3:9] + p_texts[13:51]

    # 只留下名稱跟地址的資料
    hospital_name_address = [item.split("電話：")[0] for item in p_texts]

    # 將名稱與地址資料個別儲存成兩個list
    hospital_name = [name.split("地址：")[0].strip() for name in hospital_name_address]

    hospital_address = [
        address.split("地址：")[-1].strip() for address in hospital_name_address
    ]

    # 將醫院名稱與地址放入DataFrame
    df = pd.DataFrame(
        {"hospital_name": hospital_name, "hospital_address": hospital_address}
    )
    # 儲存原始csv檔
    stc.store_to_csv_no_index(df, raw_path)

    # 執行ETL
    df["hospital_name"] = df["hospital_name"].str.split("名稱：").str[-1].str.strip()
    # 儲存ETL後的檔案
    stc.store_to_csv_no_index(df, processed_path)


if __name__ == "__main__":
    main()
