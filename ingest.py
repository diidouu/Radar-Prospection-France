import os
import sys
import requests
from datetime import datetime, timedelta
import time

URL_EXPORT = "https://bodacc-datadila.opendatasoft.com/api/explore/v2.1/catalog/datasets/annonces-commerciales/exports/json"
MAX_TENTATIVES = 3
TIMEOUT = 30

def calculer_date_cible() -> str:
    """Détermine la date cible d'ingestion.
    """
    date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    return date

def telecharger_annonce(date: str, path: str):
    """Télécharge l'annonce correspondant à la date.
    Paramètres : - date de l'export que l'on veut télécharger
                 - chemin de l'export
    """
    payload = { 'where': f"dateparution = date'{date}'" }
    for attempt in range(MAX_TENTATIVES):
        try:
            r = requests.get(URL_EXPORT, params=payload, timeout = TIMEOUT, stream=True)
            r.raise_for_status()
            with open(path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=1024*1024):
                    if chunk:
                        f.write(chunk)
                break
        
        except requests.RequestException as e:
            delai = 2 ** attempt
            print(f"Erreur : {e}")
            if attempt < MAX_TENTATIVES - 1:
                print(f"Attempt {attempt + 1} failed: {e}. Retrying in {delai} seconds ..")
                time.sleep(delai)

    else:
        raise RuntimeError(f"Failed to download the file after {MAX_TENTATIVES} attempts.")

def main():
    """Orchestre l'ingestion.
    """
    date = calculer_date_cible()
    path = f'data/bronze/bodacc_annonces_{date}/bodacc_annonces_{date}.json'

    os.makedirs(f'data/bronze/bodacc_annonces_{date}', exist_ok=True)

    try:
        telecharger_annonce(date, path)
    except RuntimeError as e:
        print(f"Erreur : {e}")
        sys.exit(1)

    size = os.path.getsize(path)
    if size <= 2:
        print("No data available for the specified date. (day with no parution?)")
    else:
        print(f"File downloaded successfully: {path}, size: {size} bytes")


if __name__ == "__main__": 
    main()

