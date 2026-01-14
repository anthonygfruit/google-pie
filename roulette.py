import requests
import json
import pandas as pd
import random
import time

WS_API_KEY = 'otq41fzv5815qr277zud81mrb6d9s5f9utujg27g' 

def get_proxies():
    url = "https://proxy.webshare.io/api/v2/proxy/list/?mode=direct"
    headers = {"Authorization": f"Token {WS_API_KEY}"}
    r = requests.get(url, headers=headers)
    data = r.json()
    proxies = []
    for p in data["results"]:
        ip = p["proxy_address"]
        port = p["port"]
        username = p["username"]
        password = p["password"] 
        proxies.append(f"http://{username}:{password}@{ip}:{port}")
        
        return proxies
    
proxy_list = get_proxies()
    
def random_proxy():
    return random.choice(proxy_list)

BASE = "https://trends.google.com/trends/api"

def trends_request(url, params=None, max_retries=5):
    for attempt in range(max_retries):
        proxy = random_proxy()
        headers = {
            "Accept": "application/json",
            "Accept-Language": "en-US,en;q=0.9",
        }

        try:
            r = requests.get(
                url,
                params=params,
                headers=headers,
                proxies={"http": proxy, "https": proxy},
                timeout=10
            )

            if r.status_code == 429:
                sleep_time = random.uniform(3, 8)
                time.sleep(sleep_time)
                continue

            if r.status_code != 200 or not r.text:
                sleep_time = random.uniform(2, 5)
                time.sleep(sleep_time)
                continue

            # remove XSSI prefix idk
            text = r.text.replace(")]}',", "")
            return json.loads(text)

        except Exception:
            time.sleep(random.uniform(2, 5))
            continue

    raise ValueError(f"Failed after {max_retries} attempts. Last response: {r.text}")

def get_explore_token(keyword, geo="", timeframe="now 7-d"):
    url = f"{BASE}/explore"
    params = {
        "hl": "en-US",
        "tz": "-480",
        "req": json.dumps({
            "comparisonItem": [{"keyword": keyword, "geo": geo, "time": timeframe}],
            "category": 0,
            "property": ""
        })
    }

    data = trends_request(url, params)

    for widget in data["widgets"]:
        if widget["id"] == "TIMESERIES":
            return widget["token"], widget["request"]

    raise ValueError("TIMESERIES widget not found")

def get_interest_over_time(keyword):
    token, req = get_explore_token(keyword)

    url = f"{BASE}/widgetdata/multiline"
    params = {
        "hl": "en-US",
        "tz": "360",
        "req": json.dumps(req),
        "token": token
    }

    data = trends_request(url, params)
    return data["default"]["timelineData"]


iot = get_interest_over_time("soccer")
for point in iot[:5]:
    print(point["time"], point["value"])
