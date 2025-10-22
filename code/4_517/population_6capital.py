<<<<<<< HEAD
import pandas as pd
import requests
import os

API_LINK = (
    "https://data.moa.gov.tw/Service/OpenData/TransService.aspx?UnitId=2thVboChxuKs"
)
raw_path = "../../data/raw/public_pet_shelter.csv"
processed_path = "../../data/processed/public_pet_shelter_ETL.csv"


def get_api_json(url: str):
    # request needs the SSL authentication but this API doesn't provide it.
    # We set the verify to False, remembering that this behavior is dangerous.
    # Do not use it in the unsafe net area.
    response = requests.get(url, verify=False)
    return response.json()


def get_df_save_raw(data, raw_path: str) -> pd.DataFrame:
    df = pd.DataFrame(data)

    # Save the original data
    os.makedirs(os.path.dirname(raw_path), exist_ok=True)
    df.to_csv(raw_path, index=False)
    print("Raw data has been saved.")
    return df


def data_process(df: pd.DataFrame, processed_path: str):
    df = df.drop(columns=["CityName", "Url", "Seq"])
    df = df.sort_values("ID")
    os.makedirs(os.path.dirname(processed_path), exist_ok=True)
    df.to_csv(processed_path, index=False)
    print("Processed data has been saved.")


def main():
    data = get_api_json(API_LINK)
    df = get_df_save_raw(data, raw_path)
    data_process(df, processed_path)

if __name__ == "__main__":
    main()
=======
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
wait = WebDriverWait(driver, 25)

try:
    print("🌐 開啟人口統計資料頁面中...")
    driver.get("https://www.ris.gov.tw/app/portal/346")
    time.sleep(3)

    # === 進入 iframe ===
    print("🕓 等待 iframe 出現中...")
    iframe = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "iframe")))
    driver.switch_to.frame(iframe)
    print("🔄 已切換進 iframe")

    # === 點擊『鄉鎮戶數及人口數(9701)』 ===
    print("🕓 等待『鄉鎮戶數及人口數(9701)』出現中...")
    btn = wait.until(EC.element_to_be_clickable((By.XPATH, "//span[contains(text(),'鄉鎮戶數及人口數(9701)')]")))
    driver.execute_script("arguments[0].click();", btn)
    print("✅ 已點擊『鄉鎮戶數及人口數(9701)』")

    # === 等待年份選單 ===
    print("🕓 等待下載設定區塊出現中...")
    wait.until(EC.presence_of_element_located((By.ID, "option-year")))
    print("✅ 偵測到年份選單")

    # === 自動選最新年月 ===
    select_year = Select(driver.find_element(By.ID, "option-year"))
    select_month = Select(driver.find_element(By.ID, "option-month"))
    latest_year = select_year.options[-1].text
    latest_month = select_month.options[-1].text
    select_year.select_by_visible_text(latest_year)
    select_month.select_by_visible_text(latest_month)
    print(f"📅 已自動選擇最新年月：{latest_year} 年 {latest_month} 月")

    # === 觸發下載 XLS ===
    driver.execute_script("""
        document.querySelectorAll('button, a, img').forEach(e=>{
            if(e.innerText.includes('XLS') || e.getAttribute('onclick')?.includes('xls')) e.click();
        });
    """)

    driver.execute_script("""
        document.querySelectorAll('button, a').forEach(e=>{
            if(e.innerText.includes('下載') || e.getAttribute('onclick')?.includes('download')) e.click();
        });
    """)

    # === 等待下載完成 ===
    time.sleep(10)
    driver.quit()
    print("🚪 已關閉瀏覽器")

    # === 找出最新下載的 XLS ===
    files = [os.path.join(download_dir, f) for f in os.listdir(download_dir) if f.endswith(".xls")]
    if not files:
        raise FileNotFoundError("找不到下載的 XLS 檔案")
    latest_file = max(files, key=os.path.getmtime)
    print(f"📁 最新下載檔案：{latest_file}")

    # === 讀取多工作表 ===
    print("📖 讀取 Excel 工作表中...")
    xls = pd.ExcelFile(latest_file)
    six_city = ["新北市", "臺北市", "桃園市", "臺中市", "臺南市", "高雄市"]
    df_all = pd.DataFrame()

    for city in six_city:
        if city not in xls.sheet_names:
            print(f"⚠️ 找不到工作表：{city}")
            continue
        df = pd.read_excel(xls, sheet_name=city, header=2)  # 從第3列開始當標題
        df = df.rename(columns={"區域別": "行政區", "計": "人口數"}, errors="ignore")
        total_row = df.iloc[0]  # 第一列是縣市總人口
        df_all = pd.concat([
            df_all,
            pd.DataFrame([[city, total_row.get("人口數", None)]], columns=["縣市", "人口數"])
        ], ignore_index=True)

    # === 匯出 CSV（含六都總人口，檔名附年月） ===
    output_name = f"six_city_population_{latest_year}{str(latest_month).zfill(2)}.csv"
    output = os.path.join(os.getcwd(), output_name)

    df_all["人口數"] = df_all["人口數"].astype("int64")
    total_population = df_all["人口數"].sum()
    df_total = pd.DataFrame([["六都合計", total_population]], columns=["縣市", "人口數"])
    df_final = pd.concat([df_all, df_total], ignore_index=True)

    df_final.to_csv(output, index=False, encoding="utf-8-sig")
    print(f"📊 已輸出六都人口數與總計：{output}")

except Exception as e:
    print(f"❌ 發生錯誤： {e}")
    driver.save_screenshot("debug_screenshot.png")
    with open("debug_page.html", "w", encoding="utf-8") as f:
        f.write(driver.page_source)
    driver.quit()
>>>>>>> 3480cf489367bd3d8e303434425f1a93a0d2155d
