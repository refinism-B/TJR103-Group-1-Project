from src.extract import E_hospital
from src.transform import (
    T_hospital_c_d,
    T_hospital_place_id,
    T_hospital_details,
    T_hospital_clean_sort,
    T_hospital_id,
    T_hospital_merge,
    T_hospital_cat_id,
    T_hospital_sql,
)
from src.load import L_hospital


def main():
    # Extract
    E_hospital.main()

    # Transform(依序執行)
    T_hospital_c_d.main()
    T_hospital_place_id.main()
    T_hospital_details.main()
    T_hospital_clean_sort.main()
    T_hospital_id.main()
    T_hospital_merge.main()
    T_hospital_cat_id.main()
    T_hospital_sql.main()

    # Load
    L_hospital.main()
