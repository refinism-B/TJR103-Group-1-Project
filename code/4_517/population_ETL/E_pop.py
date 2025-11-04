"""
E_pop.py
自動下載內政部六都人口統計資料（穩定＋容錯＋快速版）
"""

import os
import time
import requests
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager


def extract_population(download_dir: str) -> str:
    os.makedirs(download_dir, exist_ok=True)
    print("🌐 開啟內政部人口統計資料頁面中...")

    chrome_options = Options()
    chrome_options.add_experimental_option("prefs", {
        "download.default_directory": download_dir,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True
    })
    chrome_options.add_argument("--start-maximized")
    # chrome_options.add_argument("--headless=new")  # 若需無頭模式可啟用

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    wait = WebDriverWait(driver, 30)

    try:
        driver.get("https://www.ris.gov.tw/app/portal/346")
        time.sleep(3)

        # === 進入 iframe ===
        iframe = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "iframe")))
        driver.switch_to.frame(iframe)
        print("🔄 已切換進 iframe")

        # === 點擊『鄉鎮戶數及人口數(9701)』 ===
        btn = wait.until(EC.element_to_be_clickable((By.XPATH, "//span[contains(text(),'鄉鎮戶數及人口數(9701)')]")))
        driver.execute_script("arguments[0].click();", btn)
        print("✅ 已點擊『鄉鎮戶數及人口數(9701)』")

        # === 等待年份與月份下拉選單載入 ===
        print("⌛ 等待年份與月份選單載入中...")
        select_year_el = WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.ID, "option-year"))
        )
        select_month_el = WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.ID, "option-month"))
        )

        select_year = Select(select_year_el)
        select_month = Select(select_month_el)
        latest_year = select_year.options[-1].text
        latest_month = select_month.options[-1].text
        select_year.select_by_visible_text(latest_year)
        select_month.select_by_visible_text(latest_month)
        print(f"📅 已選擇最新年月：{latest_year} 年 {latest_month} 月")

        # === 嘗試取得 XLS 下載連結（預先保存）===
        xls_link = driver.execute_script("""
            let el = Array.from(document.querySelectorAll('a, button, img'))
                .find(e => e.innerText.includes('XLS') || e.getAttribute('onclick')?.includes('xls'));
            if (el && el.getAttribute('onclick')) {
                let match = el.getAttribute('onclick').match(/'(https[^']+\\.xls)'/);
                return match ? match[1] : null;
            }
            return null;
        """)
        if xls_link:
            print(f"🔗 偵測到 XLS 下載連結：{xls_link}")

        # === 嘗試 Selenium 觸發下載 ===
        driver.execute_script("""
            document.querySelectorAll('button, a, img').forEach(e=>{
                if(e.innerText.includes('XLS') || e.getAttribute('onclick')?.includes('xls')) e.click();
            });
        """)

        # === 關閉新分頁（避免 RIS 開啟新 Tab）===
        time.sleep(3)
        main_window = driver.current_window_handle
        for handle in driver.window_handles:
            if handle != main_window:
                print("🪟 偵測到新分頁，關閉中...")
                driver.switch_to.window(handle)
                driver.close()
        driver.switch_to.window(main_window)

        # === 等待檔案下載完成 ===
        print(f"💾 等待 XLS/XLSX 下載完成中...（搜尋 {download_dir}）")
        wait_time = 0
        latest_file = None
        while wait_time < 30:
            files = [os.path.join(download_dir, f) for f in os.listdir(download_dir)
                     if f.endswith((".xls", ".xlsx"))]
            if files:
                latest_file = max(files, key=os.path.getmtime)
                break
            time.sleep(1)
            wait_time += 1

        # 關閉瀏覽器
        driver.quit()

        # === 若 Selenium 下載成功 ===
        if latest_file:
            print(f"📁 最新下載檔案：{latest_file}")
            return latest_file

        # === 若 Selenium 沒成功，用 requests 直接抓取 ===
        if not xls_link:
            raise FileNotFoundError("❌ 找不到 XLS 連結，無法 fallback")

        print("⚠️ Selenium 下載未偵測到檔案，改用 requests 直接下載...")
        res = requests.get(xls_link, verify=False)
        if res.status_code == 200:
            xls_path = os.path.join(download_dir, f"population_{latest_year}_{latest_month}.xls")
            with open(xls_path, "wb") as f:
                f.write(res.content)
            print(f"📥 已成功下載至：{xls_path}")
            return xls_path
        else:
            raise Exception(f"❌ requests 下載失敗，HTTP {res.status_code}")

    except Exception as e:
        print(f"❌ 發生錯誤：{e}")
        try:
            driver.quit()
        except:
            pass
        raise
