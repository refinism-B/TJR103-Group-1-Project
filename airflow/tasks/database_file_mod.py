import pandas as pd
from pathlib import Path
from datetime import date, datetime, timedelta
import os
from dotenv import load_dotenv
from airflow.decorators import task


def create_sqlalchemy_engine():
    load_dotenv()

    username = os.getenv("MYSQL_USERNAME")
    password = os.getenv("MYSQL_PASSWORD")
    target_ip = os.getenv("MYSQL_IP")
    target_port = int(os.getenv("MYSQL_PORTT"))
    db_name = os.getenv("MYSQL_DB_NAME")

    engine = create_engine(
        f"mysql+pymysql://{username}:{password}@{target_ip}:{target_port}/{db_name}")

    return engine


@task
def L_save_file_to_csv(folder: str, file_name: str, df: pd.DataFrame) -> tuple[bool, str]:
    try:
        folder = Path(folder)
        path = folder / file_name
        df.to_csv(path, index=False, encoding="utf-8")

        print(f"{file_name}地端存檔成功！")

    except Exception as e:
        print({f"{file_name}地端存檔失敗：{e}"})


@task
def E_load_file_from_csv(folder: str, file_name: str) -> pd.DataFrame:
    folder = Path(folder)
    path = folder / file_name
    cols = pd.read_csv(path, nrows=0).columns

    dtype_dict = {col: str for col in cols if col.lower() == 'phone'}
    df = pd.read_csv(path, dtype=dtype_dict)

    return df


@task
def L_save_to_sql(df: pd.DataFrame, table_name: str, save_mod: str) -> tuple[bool, str]:
    engine = create_sqlalchemy_engine()
    try:
        df.to_sql(name=table_name,
                  con=engine, index=False, if_exists=save_mod)
        return {"result":True, "text":"成功！"}

    except Exception as e:
        return {"result":False, "text":f"{e}"}


@task
def E_load_from_sql(table_name: str) -> pd.DataFrame:
    engine = create_sqlalchemy_engine()
    sql = f"SELECT * FROM {table_name}"

    try:
        df = pd.read_sql(sql, engine)
        return df

    except Exception as e:
        raise Exception(f"讀取{table_name}表時發生錯誤：{e}")