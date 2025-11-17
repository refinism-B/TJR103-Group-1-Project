FROM apache/airflow:2.10.2-python3.12

USER root
# 安裝系統套件（可依需求增加）
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    build-essential \
    && apt-get clean

USER airflow

# 安裝 Python 套件
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
