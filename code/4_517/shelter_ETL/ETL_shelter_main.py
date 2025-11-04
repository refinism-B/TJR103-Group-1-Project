import os, logging
from datetime import datetime
from E_shelter import extract_shelter_data
from T_shelter import transform_shelter_data
from L_shelter import save_shelter_to_csv, save_shelter_to_mysql

log_dir = os.path.join(os.getcwd(), "logs")
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, f"ETL_shelter_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(log_file, encoding="utf-8"),
        logging.StreamHandler()
    ]
)

def main():
    logging.info("🐾 [START] Shelter ETL 流程開始")
    df = extract_shelter_data()
    logging.info(f"📋 共 {len(df)} 筆收容所資料")

    df = transform_shelter_data(df)
    logging.info("🧹 資料轉換完成")

    save_shelter_to_csv(df)
    save_shelter_to_mysql(df)

    logging.info("🎉 [END] Shelter ETL 流程完成")

if __name__ == "__main__":
    main()
