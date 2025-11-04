"""
ETL_pop_main.py
主控流程（整合 E → T → L）
"""

import os
from E_pop import extract_population
from T_pop import transform_population
from L_pop import load_population


def main():
    print("🏁 [ETL] 六都人口資料處理開始...")

    base_dir = os.path.dirname(__file__)
    download_dir = os.path.abspath(os.path.join(base_dir, "../../downloads"))
    output_dir = os.path.abspath(os.path.join(base_dir, "../../data"))

    # Extract
    xls_path = extract_population(download_dir)
    # Transform
    csv_path = transform_population(xls_path, output_dir)
    # Load
    load_population(csv_path)

    print("✅ [ETL] 全部流程完成！")


if __name__ == "__main__":
    main()
