import json
import os
import boto3
import pandas as pd
from io import BytesIO
from pytrends.request import TrendReq
from datetime import datetime
import botocore
import requests
import random

REPO_PATH = "/home/ec2-user/google-pie"
BUCKET = "google-search-trends"
PREFIX = "google_search/"
set_timeframe = f'2015-01-01 {datetime.today().strftime("%Y-%m-%d")}'

def load_json_dict(filename):
    with open(os.path.join(REPO_PATH, filename), "r") as f:
        return json.load(f)

def s3_file_exists(bucket, key):
    s3 = boto3.client("s3")
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return False
        else:
            raise

def get_proxies():
    url = "https://proxy.webshare.io/api/v2/proxy/list/?mode=direct"
    headers = {"Authorization": "Token otq41fzv5815qr277zud81mrb6d9s5f9utujg27g"}
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

def google_append_loop(proxy_list=None):

    cats = load_json_dict("cat_cds.json")
    geo_cds = load_json_dict("geo_cds.json")
    topic_code_dict = load_json_dict("topic_cds.json")

    s3 = boto3.client("s3")

    for topic_name, topic_id in topic_code_dict.items():
        for ctry_name, ctry_cd in geo_cds.items():
            for cat_name, cat_id in cats.items():
                time.sleep( random.randint(3, 5))

                print(f"Pulling: topic='{topic_name}' ({topic_id}), country='{ctry_name}' ({ctry_cd}), category='{cat_name}' ({cat_id})")

                # -------------------------
                # Deduplication check
                # -------------------------
                filename = f"{topic_name}_{ctry_name}_{cat_name}.parquet"
                s3_key = f"{PREFIX}{filename}"

                if s3_file_exists(BUCKET, s3_key):
                    print(f"Skipping existing: {filename}")
                    continue

                try:
                    # -------------------------
                    # Pytrends API pull
                    # -------------------------
                    if proxy_list is not None:
                        pytrend = TrendReq(hl='en-US', tz=360, proxies=proxy_list)
                    else:
                        pytrend = TrendReq(hl='en-US', tz=360)
                        
                    pytrend.build_payload(
                        kw_list=[topic_id],
                        cat=cat_id,
                        geo=ctry_cd,
                        timeframe=set_timeframe
                    )

                    df = pytrend.interest_over_time().reset_index()

                    df = df.melt(
                        id_vars=['date'],
                        value_vars=[topic_id],
                        var_name='google_id',
                        value_name='index'
                    )

                    df['country'] = ctry_name
                    df['country_iso'] = ctry_cd
                    df['topic'] = topic_name
                    df['topic_id'] = topic_id
                    df['category'] = cat_name
                    df['category_id'] = cat_id

                    # -------------------------
                    # Write Parquet to S3
                    # -------------------------
                    buffer = BytesIO()
                    df.to_parquet(buffer, index=False)
                    buffer.seek(0)

                    s3.put_object(
                        Bucket=BUCKET,
                        Key=s3_key,
                        Body=buffer.getvalue()
                    )

                    print(f"Uploaded {filename}")

                except Exception as e:
                    print(f"Error pulling {topic_id} {cat_name} {ctry_cd} >> {e}")

    return True

prox = get_proxies()
google_append_loop()
