from mods import readdata as rd
from mods import extractdata as ed
from mods import savedata as sd
from dotenv import load_dotenv


def main():
    raw_path = "data/raw/hotel/pet_establishment.csv"
    processed_path = "data/processed/hotel/hotel_data_c_d.csv"

    load_dotenv()

    df = rd.get_csv_data(raw_path)

    # 去除不必要的欄位
    df_filtered = df[
        ["legalname", "legaladdress", "busitem", "animaltype", "state_flag"]
    ]

    # busitem = A表示繁殖，C表示寄養，state_flag = N表示營業中
    df_filtered = df_filtered[
        (~df_filtered["busitem"].str.contains("A", na=False))
        & (df_filtered["busitem"].str.contains("C", na=False))
        & (df_filtered["state_flag"] == "N")
    ]

    # 找出屬於六都的機構
    df_filtered = ed.extract_city_district_from_df(df_filtered, "legaladdress")

    # 只留下名稱、地址、市和區
    df_filtered = df_filtered[["legalname", "legaladdress", "city", "district"]]

    # 修改欄位名稱
    df_filtered = df_filtered.rename(
        columns={
            "legalname": "name",
            "legaladdress": "address",
        }
    )

    sd.store_to_csv_no_index(df_filtered, processed_path)


if __name__ == "__main__":
    main()
