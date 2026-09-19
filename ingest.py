import os
import sys
import requests
from datetime import datetime, timedelta
import time

date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
payload = { 'where': f"dateparution = date'{date}'" }
path = f'data/bronze/bodacc_annonces_{date}/bodacc_annonces_{date}.json'

os.makedirs(f'data/bronze/bodacc_annonces_{date}', exist_ok=True)

for attempt in range(3):
    try:
        r = requests.get('https://bodacc-datadila.opendatasoft.com/api/explore/v2.1/catalog/datasets/annonces-commerciales/exports/json', params=payload, timeout = 30, stream=True)
        r.raise_for_status()
        with open(path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024*1024):
                if chunk:
                    f.write(chunk)
            break
    
    except requests.RequestException as e:
        sleep = 2 ** attempt
        print(f"Attempt {attempt + 1} failed: {e}. Retrying in {sleep} seconds ..")
        time.sleep(sleep)

else:
    print("Failed to download the file after 3 attempts.")
    sys.exit(1)

size = os.path.getsize(path)
if size <= 2:
     print("No data available for the specified date. (day with no parution?)")
else:
    print(f"File downloaded successfully: {path}, size: {size} bytes")


