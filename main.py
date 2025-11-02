import os
import requests
from bs4 import BeautifulSoup

base_url = "https://www.bailii.org/ky/cases/GCCI/FSD/2025/"
download_folder = "/app/data/bailii_ky"
os.makedirs(download_folder, exist_ok=True)

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/117.0.0.0 Safari/537.36"
}

r = requests.get(base_url, headers=headers)
r.raise_for_status()

soup = BeautifulSoup(r.text, "html.parser")
links = [a["href"] for a in soup.find_all("a", href=True) if a["href"].endswith(".html")]

print(f"Found {len(links)} case files")

for href in links:
    case_url = base_url + href
    case_path = os.path.join(download_folder, os.path.basename(href))
    if not os.path.exists(case_path):
        r_case = requests.get(case_url, headers=headers)
        r_case.raise_for_status()
        with open(case_path, "w", encoding="utf-8") as f:
            f.write(r_case.text)
        print(f"Downloaded {href}")
    else:
        print(f"Already exists: {href}")