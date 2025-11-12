# airflow/tasks/__init__.py
"""
主任務模組初始化：
統一匯出各任務子模組 (shelter, hospital, hotel, population, etc.)
"""

# 收容所 ETL 模組
import tasks.shelter.E_shelter as E_shelter
import tasks.shelter.T_shelter as T_shelter
import tasks.shelter.L_shelter as L_shelter

# 如果之後有其他模組，也可以在這裡加上，例如：
# import tasks.hospital.E_hospital as E_hospital
# import tasks.hotel.E_hotel as E_hotel
