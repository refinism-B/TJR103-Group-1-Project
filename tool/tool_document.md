# Tool Document
說明在本資料夾中的各個檔案或工具的功能、目的、用法。

## map_api_key
- 用於 google map API 資料爬蟲。
- 每一行為一個key，每次請使用一個key就好。
- 詳細的程式存放在 /mods/gmap.py 中。
- 檔案為.txt，在取得 key 時請使用 with/open 並用 read 模式取得。