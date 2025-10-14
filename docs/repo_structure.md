# Repository Document

這是關於這個專題Repository架構的說明文件。  
  
  
## 架構瀏覽
根目錄  
　├── docs  
　├── tool  
　├── mods  
　└── code ── personal folder  
　　　　 　　└──shared folder

## 路徑說明

- **docs**：團隊內部使用的說明文件，包括 coding style、github 規範、小組規範等。
- **tool**：專題開發所使用的工具或相關必備資訊，如連線資訊、雲端金鑰、套件等。只需要一份可供所有人使用。
- **mods**：若程式中使用到超過 2 個自訂函式，建議製作成 module 檔（其實也是 .py 檔），在程式中 import，精簡主要程式碼。
- **code**：放置主要程式碼的目錄。其中會分為個人和共用。理想上希望個人在自己的目錄中開發。確認完成後在放入共用中，避免目錄混亂。