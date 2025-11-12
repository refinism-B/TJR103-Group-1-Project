from datetime import time, date, datetime, timedelta
import pandas as pd
import ast
import numpy as np

"""
這個模組是關於時間或日期應用的自訂函式
通常簡寫為 import date_mod as dtm
"""


def count_hours(time_str: str):
    """根據時間字串計算小時（字串格式為「xx:xx - xx:xx」）"""
    step2 = time_str.replace("–", "-")
    step3 = step2.split("-")
    step4 = list(map(lambda x: x.strip(), step3))

    fmt = "%H:%M"
    start = datetime.strptime(step4[0], fmt)
    end = datetime.strptime(step4[1], fmt)

    delta = end - start
    if delta.days < 0:
        delta += timedelta(days=1)
    hours = delta.total_seconds() / 3600

    return hours


def float_or_int_converter(op_time):
    if np.isnan(op_time) or op_time < 0:
        return 0
    if op_time > 0:
        return op_time
    if op_time == 0:
        return 0


def str_converter(op_time):
    na_words = ["nan", "na", "null", "none", ]

    if op_time == "0" or op_time == "":
        return False

    if op_time.lower() in na_words:
        return False

    return True


def list_converter(op_time):
    op_time = ast.literal_eval(op_time)
    if not isinstance(op_time, list):
        return 0
    else:
        op_hours = 0

        # 營業時間為一週list，採逐日處理
        for day in op_time:
            # 使用分隔符號將"星期X"移除
            step1 = day.split(": ")[1].strip()

            # 若某日為休息則營業時間為0
            if step1 == "休息":
                hours = 0

            elif step1 == "24 小時營業":
                hours = 24

            else:
                # 若營業時間的字串中含有","表示不只一個營業時段
                # 先切割後再逐段處理
                if "," in step1:
                    op_list = step1.split(",")
                    hours = 0
                    for period in op_list:
                        period = period.strip()
                        hours += count_hours(period)
                else:
                    hours = count_hours(step1)

            op_hours += hours

    return op_hours


def trans_op_time_to_hours(op_time):
    """輸入營業時間list，會逐日計算營業時間並加總後回傳
    僅適用google map營業時間格式"""

    # 第一種狀況：輸入為None值
    if pd.isna(op_time):
        return 0

    # 第二種狀況：輸入為浮點數
    if isinstance(op_time, (int, float)):
        return float_or_int_converter(op_time)

    # 第三種情況：輸入為字串
    if isinstance(op_time, (str)):
        if not str_converter(op_time):
            return 0

        else:
            try:
                # 若有營業時間，則逐日計算時數後相加
                return list_converter(op_time)

            except (ValueError, SyntaxError):
                return 0
