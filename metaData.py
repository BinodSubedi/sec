from sec_api import QueryApi, RenderApi
import os
from pandarallel import pandarallel
from pathlib import Path
import multiprocessing
import pandas as pd

# load Russell 3000 holdings CSV into a dataframe
holdings = pd.read_csv('./russell-3000-clean.csv')


API_KEY = "9a6033dae225dc7b78d9c6b20495e65c8d0bef5da2ec97cc3ed2d8955655d050"
API_ENDPOINT = "https://api.sec-api.io/filing-reader"

queryApi = QueryApi(api_key=API_KEY)
renderApi = RenderApi(api_key=API_KEY)



# create batches of tickers: [[A,B,C], [D,E,F], ...]
# a single batch has a maximum of max_length_of_batch tickers
def create_batches(tickers = [], max_length_of_batch = 100):
  batches = [[]]

  for ticker in tickers:
    if len(batches[len(batches)-1]) == max_length_of_batch:
      batches.append([])

    batches[len(batches)-1].append(ticker)

  return batches


#batches = create_batches(list(holdings['Ticker']))



def download_10K_metadata(tickers = [], start_year = 2023, end_year = 2023):
  # if Path('metadata.csv').is_file():
  #   print('✅ Reading metadata from metadata.csv')
  #   result = pd.read_csv('metadata.csv')
  #   return result

  print('✅ Starting downloading metadata for years {} to {}'.format(start_year, end_year))

  # create ticker batches, with 100 tickers per batch
  batches = create_batches(tickers)
  frames = []

  for year in range(start_year, end_year + 1):
    for batch in batches:
      tickers_joined = ', '.join(batch)
      ticker_query = 'ticker:({})'.format(tickers_joined)

      query_string = '{ticker_query} AND filedAt:[{start_year}-01-01 TO {end_year}-12-31] AND formType:"10-K" AND NOT formType:"10-K/A" AND NOT formType:NT'.format(ticker_query=ticker_query, start_year=year, end_year=year)

      query = {
        "query": { "query_string": { 
            "query": query_string,
            "time_zone": "America/New_York"
        } },
        "from": "0",
        "size": "200",
        "sort": [{ "filedAt": { "order": "desc" } }]
      }

      response = queryApi.get_filings(query)

      filings = response['filings']

      metadata = list(map(lambda f: {'ticker': f['ticker'], 
                                     'cik': f['cik'], 
                                     'formType': f['formType'], 
                                     'filedAt': f['filedAt'], 
                                     'filingUrl': f['linkToFilingDetails']}, filings))

      df = pd.DataFrame.from_records(metadata)

      frames.append(df)

    print('✅ Downloaded metadata for year', year)


  result = pd.concat(frames)
  result.to_csv('metadata.csv', index=False)

  number_metadata_downloaded = len(result)
  print('✅ Download completed. Metadata downloaded for {} filings.'.format(number_metadata_downloaded))

  return result


tickers = list(holdings['Ticker'])

metadata = download_10K_metadata(tickers=tickers, start_year=2023, end_year=2023)

metadata.head(10)


def download_filing(metadata):
  try:
    ticker = metadata['ticker']
    new_folder = "./filings/" + ticker

    if not os.path.isdir(new_folder):
      os.makedirs(new_folder)

    url = metadata['filingUrl'].replace('ix?doc=/', '')
    api_url = API_ENDPOINT + "?token=" + API_KEY + "&url=" + url + "&type=pdf"
    #file_content = renderApi.get_filing(url)
    response = requests.get(api_url) 
    
    file_name = url.split("/")[-1].split('.')[0] + '.pdf' 

    with open(new_folder + "/" + file_name, "w") as f:
      f.write(response.content)
  except:
     print('❌ {ticker}: downloaded failed: {url}'.format(ticker=ticker, url=url))



number_of_workers = 4
pandarallel.initialize(progress_bar=True, nb_workers=number_of_workers, verbose=0)

# uncomment to run a quick sample and download 50 filings
sample = metadata.head(50)
sample.parallel_apply(download_filing, axis=1)

# download all filings 
# metadata.parallel_apply(download_filing, axis=1)

print('✅ Download completed')
