from mods import get_var_data as gd
from mods import store_to_csv as stc
from thefuzz import fuzz


# 設定兩個醫院csv檔的路徑
hospital_path = "data/processed/hospital_data_ETL.csv"
hospital_24_path = "data/processed/hospital_24_hr_data_ETL.csv"


# 建立模糊比對的方法
def match_fuzzy(name, address, df2, name_threshold=67, address_threshold=77):
    for _, row in df2.iterrows():
        name_score = fuzz.token_set_ratio(name, row["hospital_name"])
        address_score = fuzz.token_set_ratio(address, row["hospital_address"])
        if name_score >= name_threshold and address_score >= address_threshold:
            return True
    return False


# 讀取csv檔並建立兩張df
hospital_normal = gd.get_csv_data(path=hospital_path)
hospital_24 = gd.get_csv_data(path=hospital_24_path)


# 方法1
# 呼叫模糊比對方法，需手動調整比對分數
hospital_normal["opening_hour"] = hospital_normal.apply(
    lambda x: match_fuzzy(x["name"], x["address"], hospital_24), axis=1
)

# 方法2
# 使用isin()方式比對，需資料完全吻合
hospital_normal["opening_hour"] = (
    hospital_normal["name"].isin(hospital_24["hospital_name"])
) | (hospital_normal["address"].isin(hospital_24["hospital_address"]))


# 儲存df
target_path = "data/processed/hospital_all_ETL.csv"
stc.store_to_csv(hospital_normal, target_path)
