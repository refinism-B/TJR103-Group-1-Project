from src.extract import E_hotel
from src.transform import (
    T_hotel_c_d,
    T_hotel_place_id,
    T_hotel_details,
    T_hotel_clean_sort,
    T_hotel_id,
    T_hotel_merge,
    T_hotel_cat_id,
    T_hotel_sql,
)
from src.load import L_hotel


def main():
    # Extract
    E_hotel.main()

    # Transform(依序執行)
    T_hotel_c_d.main()
    T_hotel_place_id.main()
    T_hotel_details.main()
    T_hotel_clean_sort.main()
    T_hotel_id.main()
    T_hotel_merge.main()
    T_hotel_cat_id.main()
    T_hotel_sql.main()

    # Load
    L_hotel.main()
