from selenium import webdriver
from selenium.webdriver.edge.options import Options
import mods.get_pet_hospital as get_hospital


URL = "https://ahis9.aphia.gov.tw/Veter/OD/HLIndex.aspx"
raw_path = "data/raw/hospital_data.csv"
processed_path = "data/processed/hospital_data_ETL.csv"

# 對edge的options加上headers
options = Options()
options.add_argument("user-agent=MyAgent/1.0")

# 設定edge的driver
driver = webdriver.Edge(options=options)


def main():
    soup = get_hospital.get_into_webpage_html(url=URL, driver=driver)
    df = get_hospital.get_hospital_data_save(soup, raw_path)
    get_hospital.data_process(df, processed_path)


if __name__ == "__main__":
    main()
