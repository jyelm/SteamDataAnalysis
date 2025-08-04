import requests, re, pandas as pd
from bs4 import BeautifulSoup

url = "https://steamcharts.com/top"
soup = BeautifulSoup(requests.get(url, headers={"User-Agent":"Mozilla/5.0"}).text, "html.parser")
games = [(int(a['href'].split('/')[-1]), a.text.strip())          # (appid, name)
         for a in soup.select("table#top-games a[href^='/app/']")]

print(games)
