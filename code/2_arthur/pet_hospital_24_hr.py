import mods.get_pet_hospital_24_hr as get_24

# 設定URL和headers
URL = "https://www.dogcatstar.com/blog/24hour-animal-hospital/?srsltid=AfmBOorT4sCf-7J95E1GuTg29XR3w-xYXl2qm-PZL1eOjPJ0k3ndzEem"
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36 Edg/141.0.0.0"
}
raw_path = "data/raw/hospital_24_hr_data.csv"
processed_path = "data/processed/hospital_24_hr_data_ETL.csv"


def main():
    soup = get_24.get_the_html(url=URL, headers=headers)
    hospital_name, hospital_address = get_24.get_name_address(soup=soup)
    df = get_24.to_dataframe_save(hospital_name, hospital_address, raw_path)
    get_24.data_process(df=df, processed_path=processed_path)


if __name__ == "__main__":
    main()
