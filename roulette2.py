from pytrends.request import TrendReq
import pandas as pd

# Minimal setup
topic_id = 'soccer'
country_code = 'US'
category_id = None
set_timeframe = '2017-01-01 2025-12-31'

# Minimal pytrends session (no proxy)
pytrend = TrendReq(hl='en-US', tz=360)

# Build payload and fetch data
pytrend.build_payload(kw_list=[topic_id], cat=category_id, geo=country_code, timeframe=set_timeframe)
df = pytrend.interest_over_time().reset_index()

# Melt and add minimal columns
df = df.melt(id_vars=['date'], value_vars=[topic_id], var_name='google_id', value_name='index')
df['country'] = country_code
df['topic_id'] = topic_id
df['category_id'] = category_id

# print df
print(df.head())
