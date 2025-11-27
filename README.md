# TJR103 雲端資料工程師班 第一組專題：寵物資訊站

![Python](https://img.shields.io/badge/python-3.11-blue) ![MySQL](https://img.shields.io/badge/Database-MySQL-4479A1) ![Apache Airflow](https://img.shields.io/badge/Workflow-Airflow-017CEE) ![Google Cloud](https://img.shields.io/badge/Cloud-GCP-4285F4) ![License](https://img.shields.io/badge/license-MIT-green) ![Contributors](https://img.shields.io/github/contributors/refinism-B/TJR103-Group-1-Project) ![Data Analysis](https://img.shields.io/badge/Data%20Analysis-Complete-brightgreen)

## 目錄
- [Overview](#專案概要-overview)
- [Motivation](#研究動機-motivation)
- [Research Scope](#研究範圍-research-scope)
- [Data Sources](#資料來源-data-sources)
- [Architecture](#技術架構與流程-architecture)
- [Data Analysis & Metrics](#核心指標與分析模型-data-analysis--metrics)
- [Challenges](#技術挑戰與解決方案-challenges)
- [Future Roadmap](#未來展望-future-roadmap)



## 專案概要 (Overview)

本專題針對台灣六都寵物資源進行資料蒐集與分析，建立一個**寵物資訊集中平台**。我們不僅提供資源查詢，更透過獨創的**雙向市場地圖（Two-sided Index Map）**，幫助飼主評估居住便利性，同時協助業者判斷商業潛力。

## 研究動機 (Motivation)

1. **飼主角度**：展示六都各地寵物資源數比較，解決資訊破碎問題。
2. **業者角度**：分析六都各地區的商業潛力，提供選址與展店的數據支持。
3. **平台價值**：建立一個**寵物資源匯集平台**，實現資訊透明化。

## 研究範圍 (Research Scope)

1. **研究範圍**：以六都為研究對象，最小行政單位為「區」。
2. **店家分類**：分為「**寵物用品**」、「**寵物餐廳**」、「**寵物美容**」、「**寵物旅館**」、「**寵物醫院**」、「**寵物收容所**」等六大類。


## 資料來源 (Data Sources)

| 資料類別 | 資料來源 | 用途 |
| :--- | :--- | :--- |
| 寵物登記數 | [農業部寵物登記管理資訊網](https://www.pet.gov.tw/Web/O302.aspx) | 計算區域寵物密度、市場需求基數 |
| 公立收容所 | [動物保護資訊網](https://animal.moa.gov.tw/Frontend/PublicShelter) | 評估區域公益資源 |
| 人口與行政區 | 內政部戶政司 | 計算人口密度、人寵比 |
| 寵物醫院 | [農業部動植物防疫檢疫署](https://ahis9.aphia.gov.tw/Veter/OD/HLIndex.aspx) | 醫療資源可達性分析 |
| 寵物旅館 | [農業部寵物登記管理資訊網](https://www.pet.gov.tw/Web/BusinessList.aspx) | 合法業者名單校對 |
| 其他店家 | Google Maps API | 獲取評分、評論數、營業時間等動態資料 |


## 技術架構與流程 (Architecture)

1. **資料蒐集**：使用 **Python 爬蟲套件** 及 **Google Maps API** 蒐集店家資訊。
2. **ETL 處理**：透過 **Pandas** 進行資料清洗、型別轉換及異常值剔除。
3. **Data Pipeline 自動化**：將專案部署於 **GCP VM**，利用 **Apache Airflow** 實現排程自動化。
4. **資料儲存**：在 GCP VM 中部署 **MySQL**，作為核心資料倉儲。
5. **視覺化與展示**：使用 **Tableau** 製作互動式儀表板 (Dashboard)。

![專題架構圖](https://i.meee.com.tw/QTjrWsu.png)


## 核心指標與分析模型 (Data Analysis & Metrics)

本專案最大的特色在於建立了兩套核心指標，並結合形成「台灣雙向寵物市場地圖」：

- **PCI (Pet Convenience Index)**：**使用者端指標**。衡量「養寵物的人住在這個區域方便嗎？」
- **SAI (Store Attraction Index)**：**商家端指標**。衡量「這個區域適不適合開一間寵物店？」

### 1. PCI：寵物生活便利指數模型

PCI 的計算經歷了五個嚴謹的資料處理步驟，以確保評分的公正性與參考價值：

*   **Step 1: Store Score（單店評分）**
    利用貝葉斯平滑（Bayesian Smoothing）修正單純平均星等的偏差，避免新店因評論數過少而產生極端分數。
    > 公式概念：`店家評分 = 品質(Rating) × 評價可信度(Bayes) × 可用性(營業時數)`

*   **Step 2: Category Raw Score（區域類別原始分）**
    計算單一行政區內，特定類別（如醫院）的整體服務能量。
    > 公式概念：`區域類別分 = Σ 店家便利度 ÷ (每萬隻寵物數)`

*   **Step 3: Normalization（P10-P90 標準化）**
    為避免極端值影響整體排名，我們採用 P10-P90 去除離群值，並將分數映射至 `0.5 ~ 9.5` 區間。

*   **Step 4: Weighted Score（類別權重配置）**
    根據飼主需求的頻率與重要性，對不同類別店家設定權重：

    | 店家類別 | 權重占比 | 考量因素 |
    | :--- | :--- | :--- |
    | 一般醫院 | **27%** | 醫療為最核心剛需 |
    | 24h 急診醫院 | **22%** | 緊急醫療的可達性 |
    | 寵物美容 | **15%** | 定期護理需求 |
    | 寵物用品 | **13%** | 日常消耗品採購 |
    | 寵物旅館 | **11%** | 寄宿與安親需求 |
    | 寵物餐廳 | **9%** | 休閒與社交需求 |
    | 收容所 | **3%** | 公益與認養資源 |

*   **Step 5: Final PCI Calculation**
    最終分數結合了「市內相對分數」與「六都整體分數」，並進行滿分校正。
    > `Final PCI = (70% 市內排名分 + 30% 六都排名分) × 校正係數`

### 2. SAI：開店吸引力指數模型

SAI 是一個輔助商業決策的選址模型，由三大構面組成：

> **SAI Formula** = $(市場潛力)^{0.4} \times (1 - 競爭壓力)^{0.3} \times (產業成熟度)^{0.3}$

*   **市場潛力**：由人口密度、寵物登記數、區域寵物文化構成。
*   **競爭壓力**：分析同類別店家的密度，密度越高，競爭壓力越大（分數越低）。
*   **產業成熟度**：參考 PCI 指數，反映該區現有的商業群聚效應。

### 3. 雙向市場地圖 (Two-sided Index Map)

將 PCI 與 SAI 交叉比對，我們將六都的行政區劃分為四種市場類型，提供不同的策略建議：

| 類型 | 定義 | 市場含義 | 建議策略 |
| :--- | :--- | :--- | :--- |
| **成熟核心區** | High PCI × High SAI | 生活機能極佳，且仍具商業吸引力 | **穩定拓點**：適合品牌旗艦店進駐 |
| **飽和高競爭區** | High PCI × Low SAI | 生活方便，但店家過多，競爭激烈 | **精準差異化**：需強調特色服務才能生存 |
| **成長潛力區** | Low PCI × High SAI | 生活機能尚缺，但具備高市場需求 | **先行者優勢**：適合搶先佈局，填補缺口 |
| **低活性區** | Low PCI × Low SAI | 需求低且現有資源少 | **暫緩佈局**：需等待區域發展成熟 |


## 技術挑戰與解決方案 (Challenges)

### 1. 店家資訊蒐集
- **挑戰**：網上並無完整、集中的寵物店家資訊。
- **解法**：整合政府公開資料與 **Google Maps API**，建立最完整的資料庫。

### 2. API 成本控制
- **挑戰**：大量呼叫 Google Maps API 可能產生高額費用。
- **解法**：實作快取機制（Caching），紀錄已搜尋過的參數與結果，大幅降低重複查詢成本。

### 3. 雲端環境部署 (Dockerization)
- **挑戰**：地端開發環境 (Windows/Mac) 與雲端生產環境 (Linux VM) 差異導致程式運行錯誤。
- **解法**：
   - 容器化開發：使用 **Docker** 統一開發與部署環境。
   - 版本控制：透過 **GitHub** 進行程式碼同步。
   - 部署流程：Local Dev -> Git Push -> VM Pull -> Docker Run。

![雲端部署流程](https://i.meee.com.tw/gj2lXhj.png)

## 未來展望 (Future Roadmap)

考量專案時程，本專案仍有優化空間，未來預計進行以下升級：

1.  **導入 BigQuery (OLAP)**
    *   目前使用 MySQL (OLTP)，未來將資料匯入 **Google BigQuery**，以提升大數據分析的查詢效能，並降低維護成本。
2.  **CI/CD 自動化整合**
    *   導入 GitHub Actions 或 Jenkins，實現從 Code Push 到 Cloud Deployment 的全自動化流程，減少手動 SSH 連線操作。
