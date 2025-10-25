import os
import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager

# === 設定下載資料夾 ===
download_dir = os.path.join(os.getcwd(), "downloads")
os.makedirs(download_dir, exist_ok=True)

chrome_options = Options()
chrome_options.add_experimental_option("prefs", {
    "download.default_directory": download_dir,
    "download.prompt_for_download": False,
    "download.directory_upgrade": True,
    "safebrowsing.enabled": True
})
chrome_options.add_argument("--start-maximized")

driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
wait = WebDriverWait(driver, 30)

try:
    print("🌐 開啟內政部人口統計資料頁面中...")
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

    # === 選取最新年月 ===
    select_year = Select(driver.find_element(By.ID, "option-year"))
    select_month = Select(driver.find_element(By.ID, "option-month"))
    latest_year = select_year.options[-1].text
    latest_month = select_month.options[-1].text
    select_year.select_by_visible_text(latest_year)
    select_month.select_by_visible_text(latest_month)
    print(f"📅 已選擇最新年月：{latest_year} 年 {latest_month} 月")

    # === 觸發 XLS 檔案下載 ===
    driver.execute_script("""
        document.querySelectorAll('button, a, img').forEach(e=>{
            if(e.innerText.includes('XLS') || e.getAttribute('onclick')?.includes('xls')) e.click();
        });
    """)
    time.sleep(10)
    driver.quit()
    print("🚪 已關閉瀏覽器")

    # === 找出最新下載檔案 ===
    files = [os.path.join(download_dir, f) for f in os.listdir(download_dir) if f.endswith(".xls")]
    if not files:
        raise FileNotFoundError("❌ 找不到下載的 XLS 檔案！")
    latest_file = max(files, key=os.path.getmtime)
    print(f"📁 最新下載檔案：{latest_file}")

    # === 六都各自的擷取行數（從第4列開始） ===
    city_rows = {
        "臺北市": 12,
        "新北市": 31,
        "桃園市": 13,
        "臺中市": 29,
        "臺南市": 37,
        "高雄市": 38
    }

    print("📖 開始解析各縣市資料...")
    xls = pd.ExcelFile(latest_file)
    df_all = pd.DataFrame()

    for city, row_count in city_rows.items():
        if city not in xls.sheet_names:
            print(f"⚠️ 找不到工作表：{city}")
            continue

        df = pd.read_excel(
            xls,
            sheet_name=city,
            skiprows=4,  # ✅ 從第5列開始讀，保留第5列行政區
            nrows=row_count,
            usecols="A:C",
            header=None,
            names=["行政區", "戶數", "人口數"]
        )

        df = df.dropna(subset=["行政區"])
        df = df[~df["行政區"].astype(str).str.contains("合計|總計|註|^說明")]
        df["行政區"] = df["行政區"].astype(str).str.replace("※", "").str.strip()
        df["人口數"] = pd.to_numeric(df["人口數"].astype(str).str.replace(",", ""), errors="coerce").fillna(0).astype(int)
        df.insert(0, "縣市", city)
        df = df[["縣市", "行政區", "人口數"]]

        df_all = pd.concat([df_all, df], ignore_index=True)
        print(f"✅ {city} 已擷取 {len(df)} 筆資料")

    # === 檢查總筆數 ===
    total_rows = len(df_all)
    if total_rows != 158:
        print(f"⚠️ 資料筆數不符：目前為 {total_rows} 筆，預期為 158 筆")
    else:
        print("✅ 資料筆數正確，共 158 筆")

    # === 匯出 CSV ===
    output_name = f"six_city_population_{latest_year}{str(latest_month).zfill(2)}.csv"
    df_all.to_csv(output_name, index=False, encoding="utf-8-sig")

    print(f"📦 已成功輸出六都人口數：{output_name}")
    print(df_all.head(10))

except Exception as e:
    print(f"❌ 發生錯誤：{e}")
    try:
        driver.quit()
    except:
        pass

    