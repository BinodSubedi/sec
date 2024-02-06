import os
from sec_api import QueryApi, RenderApi
import requests
from pandarallel import pandarallel
from pathlib import Path
import multiprocessing
import pandas as pd

API_KEY = "6693ab456a9c2c85e36d114d62e6519478eb8e150c4cfaa93a542c06b0b1ce0d"
API_ENDPOINT = "https://api.sec-api.io/filing-reader"

queryApi = QueryApi(api_key=API_KEY)
renderApi = RenderApi(api_key=API_KEY)

metadata = pd.read_csv('metadata.csv')

def download_filing(data):
    try:
        ticker = data['ticker']
        new_folder = "./filings/" + ticker

        if not os.path.isdir(new_folder):
            os.makedirs(new_folder)

        url = data['filingUrl'].replace('ix?doc=/', '')
        api_url = API_ENDPOINT + "?token=" + API_KEY + "&url=" +  url + "&type=pdf"
        print(api_url)
        response = requests.get(api_url)

        file_name = url.split("/")[-1].split('.')[0] + '.pdf'

        with open(new_folder + "/" + file_name, "wb") as f:
            f.write(response.content)
    except:
        print('❌ {ticker}: downloaded failed: {url}'.format(ticker=ticker, url=url))


#download_filing(metadata.iloc[0])

number_of_workers = 4
pandarallel.initialize(progress_bar=True, nb_workers=number_of_workers, verbose=0)

# uncomment to run a quick sample and download 50 filings
sample = metadata.head(50)
sample.parallel_apply(download_filing, axis=1)

# download all filings 
# metadata.parallel_apply(download_filing, axis=1)

print('✅ Download completed')

