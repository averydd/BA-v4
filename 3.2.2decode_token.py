import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import time

df = pd.read_csv("token_pairs_output.csv")

options = webdriver.ChromeOptions()
options.headless = True

driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)


def get_token_name(contract_address):
    url = f"https://etherscan.io/address/{contract_address}"
    driver.get(url)
    time.sleep(5)

    soup = BeautifulSoup(driver.page_source, "html.parser")

    try:
        token_name_tag = soup.find("div", {"id": "ContentPlaceHolder1_tr_tokeninfo"}).find("a")
        token_name = token_name_tag.get_text(strip=True) if token_name_tag else "Unknown"

        return token_name
    except AttributeError:
        return "Unknown"


names_token0 = []
names_token1 = []

for index, row in df.iterrows():
    contract_address1 = row['token0']
    contract_address2 = row['token1']

    name_token0 = get_token_name(contract_address1)  # Crawl token0
    name_token1 = get_token_name(contract_address2)  # Crawl token1

    names_token0.append(name_token0)
    names_token1.append(name_token1)

df['name_token0'] = names_token0
df['name_token1'] = names_token1

df.to_csv("token_pairs_with_names.csv", index=False)

driver.quit()

print("Crawl hoàn tất và dữ liệu đã được lưu vào 'token_pairs_with_names.csv'")
