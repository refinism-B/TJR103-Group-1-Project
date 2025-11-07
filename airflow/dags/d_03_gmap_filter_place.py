import ast
import os
from datetime import date, datetime, timedelta

from airflow.decorators import dag, task
import geopandas as gpd
import pandas as pd
from dotenv import load_dotenv
from shapely.geometry import Point
from sqlalchemy import create_engine
from pathlib import Path


# 設定DAG基本資訊
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["your_email@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}


@dag(
    dag_id="d_test_gmap_filter_place",
    default_args=default_args,
    description="[每日更新]爬取每日寵物登記數",
    schedule_interval="*/45 * * * *",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["bevis", "daily"]  # Optional: Add tags for better filtering in the UI
)



def d_03_gmap_filter_place():