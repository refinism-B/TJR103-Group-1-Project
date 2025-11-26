import os
import time

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select, WebDriverWait


def fetch_population_data(raw_dir):
    """
    修正版：強制所有 Selenium 下載在 /downloads
    並從 /downloads 移到 Airflow 的 raw_dir。
    """

    # === Container 統一路徑 ===
    selenium_download_dir = "/downloads"
    # os.makedirs(selenium_download_dir, exist_ok=True)
    os.makedirs(raw_dir, exist_ok=True)

    # === Chrome 選項設定 ===
    chrome_options = Options()
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")

    chrome_options.add_experimental_option(
        "prefs",
        {
            "download.default_directory": selenium_download_dir,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True,
        },
    )

    # Remote Driver
    selen_url = "http://35.194.236.122:14444/wd/hub"

    with webdriver.Remote(command_executor=selen_url, options=chrome_options) as driver:
        wait = WebDriverWait(driver, 30)

        print("🌐 開啟人口統計頁面...")
        driver.get("https://www.ris.gov.tw/app/portal/346")

        wait.until(
            EC.frame_to_be_available_and_switch_to_it((By.CSS_SELECTOR, "iframe"))
        )

        # 點擊資料項目
        btn = wait.until(
            EC.element_to_be_clickable(
                (By.XPATH, "//span[contains(text(),'鄉鎮戶數及人口數(9701)')]")
            )
        )
        driver.execute_script("arguments[0].click();", btn)
        print("✅ 已點擊資料項目")

        # 年月
        select_year = Select(driver.find_element(By.ID, "option-year"))
        select_month = Select(driver.find_element(By.ID, "option-month"))

        latest_year = select_year.options[-1].text
        latest_month = select_month.options[-1].text
        select_year.select_by_visible_text(latest_year)
        select_month.select_by_visible_text(latest_month)

        print(f"📅 已選擇最新年月：{latest_year} 年 {latest_month} 月")

        # XLS radio
        xls_radio = wait.until(
            EC.element_to_be_clickable((By.XPATH, "//input[@value='xls']"))
        )
        driver.execute_script("arguments[0].click();", xls_radio)

        # 下載
        download_btn = wait.until(
            EC.element_to_be_clickable((By.XPATH, "//button[contains(text(),'下載')]"))
        )
        driver.execute_script("arguments[0].click();", download_btn)
        print("⬇️ 開始下載 XLS 檔案...")

        # ======= 等待下載完成 =======
        downloaded = None
        for _ in range(120):  # 最長 120 秒
            files = [f for f in os.listdir(raw_dir) if f.endswith(".xls")]
            partials = [f for f in os.listdir(raw_dir) if f.endswith(".crdownload")]

            if files and not partials:
                downloaded = max(
                    files,
                    key=lambda f: os.path.getmtime(os.path.join(raw_dir, f)),
                )
                break

            time.sleep(1)

        if not downloaded:
            raise FileNotFoundError("❌ 未找到下載完成的 XLS 檔案")

        # src = os.path.join(raw_dir, downloaded)
        dst = os.path.join(raw_dir, downloaded)

        # # 移動到 Airflow 專案資料夾
        # os.replace(src, dst)

        print(f"📁 完成下載：{dst}")

        return dst, f"{latest_year}-{latest_month}"


def fetch_raw_data(raw_dir):
    return fetch_population_data(raw_dir)
