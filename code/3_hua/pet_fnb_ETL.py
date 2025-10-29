import pandas as pd
import re

# 讀取原始csv檔案
filename = (r"C:\Users\Vincent\Desktop\ETL_test.csv")
df = pd.read_csv(filename, dtype={"phone": str})


# 清理已歇業或暫時停業的店家，只留下"business_status"為"OPERATIONAL"
df = df[df["business_status"] == "OPERATIONAL"].copy()


# 移除"address"的郵遞區號與台灣
def clean_address(addr):
    try :
        addr = addr.split("台灣")[-1].strip()
        return addr
    except Exception as e:
        print(f"地址有誤：{addr}（錯誤原因：{e}）")
        return None
    
df["address"] = df["address"].apply(clean_address)


# 將地址拆出縣市欄位
def city(addr):
    try :
        c = re.search(r"(.{1,2}(縣|市))", addr)
        return c.group(1) if c else None
    except Exception as e:
        print(f"有誤：{addr}（錯誤原因：{e}）")
        return None

df.insert(3, "city", df["address"].apply(city))


# 將地址拆出鄉鎮市區欄位
def district(addr):
    try :
        d = re.search(r"(?:縣|市)(.{1,3}(鄉|鎮|市|區))", addr)
        return d.group(1) if d else None
    except Exception as e:
        print(f"有誤：{addr}（錯誤原因：{e}）")
        return None
    
df.insert(4, "district", df["address"].apply(district))


# 將電話空值補上"unknown"
df["phone"] = df["phone"].fillna("unknown").str.strip()