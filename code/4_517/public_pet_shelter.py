import pandas as pd
import requests
import os

API_LINK = (
    "https://data.moa.gov.tw/Service/OpenData/TransService.aspx?UnitId=2thVboChxuKs"
)
raw_path = "../../data/raw/public_pet_shelter.csv"
processed_path = "../../data/processed/public_pet_shelter_ETL.csv"


def get_api_json(url: str):
    # request needs the SSL authentication but this API doesn't provide it.
    # We set the verify to False, remembering that this behavior is dangerous.
    # Do not use it in the unsafe net area.
    response = requests.get(url, verify=False)
    return response.json()


def get_df_save_raw(data, raw_path: str) -> pd.DataFrame:
    df = pd.DataFrame(data)

    # Save the original data
    os.makedirs(os.path.dirname(raw_path), exist_ok=True)
    df.to_csv(raw_path, index=False)
    print("Raw data has been saved.")
    return df


def data_process(df: pd.DataFrame, processed_path: str):
    df = df.drop(columns=["CityName", "Url", "Seq"])
    df = df.sort_values("ID")
    os.makedirs(os.path.dirname(processed_path), exist_ok=True)
    df.to_csv(processed_path, index=False)
    print("Processed data has been saved.")


def main():
    data = get_api_json(API_LINK)
    df = get_df_save_raw(data, raw_path)
    data_process(df, processed_path)


if __name__ == "__main__":
    main()
    
    