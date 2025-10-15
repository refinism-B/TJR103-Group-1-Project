from selenium import webdriver
from bs4 import BeautifulSoup
import pandas as pd
import time

driver = webdriver.Chrome()
driver.get("https://animal.moa.gov.tw/Frontend/AdoptSearch/AdoptInfo")
time.sleep(5)

soup = BeautifulSoup(driver.page_source, "html.parser")

table = soup.find("table", class_="table table-hover")
rows = table.find_all("tr") if table else []

data = []
for row in rows[1:]:
    cols = row.find_all("td")
    if len(cols) >= 3:
        num = cols[0].get_text(strip=True)
        name = cols[1].get_text(strip=True)
        contact_html = cols[2]
        links = [a['href'] for a in contact_html.find_all('a', href=True)]
        phones = [a.get_text(strip=True) for a in contact_html.find_all('a', href=True) if a['href'].startswith('tel')]
        addr = None
        for i in contact_html.find_all("i", class_="fa-map-marker"):
            addr = i.next_sibling.strip() if i.next_sibling else None

        data.append({
            "serial_number": num,
            "org_name": name,
            "url": links[0] if links else "",
            "address": addr,
            "phone_number": "、".join(phones)
        })


driver.quit()


df = pd.DataFrame(data)
csv_filename = "animal_protection.csv"
df.to_csv(csv_filename, index=False, encoding="utf-8-sig")