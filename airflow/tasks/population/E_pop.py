import os
import time
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager


def fetch_population_data(raw_dir):
    """
    使用 Selenium 自動下載內政部人口統計 XLS 檔案，並回傳檔案路徑與年月。
    """
    chrome_options = Options()
    chrome_options.add_experimental_option("prefs", {
        "download.default_directory": raw_dir,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True
    })
    chrome_options.add_argument("--headless")  # 可選：無頭模式
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    wait = WebDriverWait(driver, 30)

    try:
        print("🌐 開啟內政部人口統計資料頁面中...")
        driver.get("https://www.ris.gov.tw/app/portal/346")
        time.sleep(3)

        # === 進入 iframe ===
        iframe = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "iframe")))
        driver.switch_to.frame(iframe)

        # === 點擊『鄉鎮戶數及人口數(9701)』 ===
        btn = wait.until(EC.element_to_be_clickable((By.XPATH, "//span[contains(text(),'鄉鎮戶數及人口數(9701)')]")))
        driver.execute_script("arguments[0].click();", btn)
        print("✅ 已點擊『鄉鎮戶數及人口數(9701)』")

        # === 選擇最新年月 ===
        select_year = Select(driver.find_element(By.ID, "option-year"))
        select_month = Select(driver.find_element(By.ID, "option-month"))
        latest_year = select_year.options[-1].text
        latest_month = select_month.options[-1].text
        select_year.select_by_visible_text(latest_year)
        select_month.select_by_visible_text(latest_month)
        print(f"📅 已選擇最新年月：{latest_year} 年 {latest_month} 月")

        # === 選擇 XLS 格式 ===
        print("📄 選擇 XLS 格式...")
        xls_radio = wait.until(EC.element_to_be_clickable((By.XPATH, "//input[@value='xls']")))
        driver.execute_script("arguments[0].click();", xls_radio)
        time.sleep(1)

        # === 點擊下載 ===
        print("⬇️ 點擊『下載』按鈕...")
        download_btn = wait.until(EC.element_to_be_clickable((By.XPATH, "//button[contains(text(),'下載')]")))
        driver.execute_script("arguments[0].click();", download_btn)

        # === 等待下載完成 ===
        print("⌛ 等待 XLS 檔案下載中...")
        timeout = 40
        latest_file = None
        for _ in range(timeout):
            files = [f for f in os.listdir(raw_dir) if f.endswith(".xls")]
            if files:
                latest_file = max(files, key=lambda f: os.path.getmtime(os.path.join(raw_dir, f)))
                break
            time.sleep(1)

        driver.quit()
        print("🚪 已關閉瀏覽器")

        if not latest_file:
            raise FileNotFoundError("❌ 找不到下載的 XLS 檔案！")

        full_path = os.path.join(raw_dir, latest_file)
        print(f"📁 最新下載檔案：{full_path}")
        return full_path, latest_year, latest_month

    except Exception as e:
        print(f"❌ 抓取人口資料失敗：{e}")
        try:
            driver.quit()
        except:
            pass
        return None, None, None
    
def fetch_raw_data(raw_dir):
    return fetch_population_data(raw_dir)