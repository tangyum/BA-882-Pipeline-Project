# imports

import yfinance as yf
import pandas as pd
from datetime import datetime
from google.cloud import storage
import uuid
from io import BytesIO
import functions_framework



# settings
project_id = 'ba882-435919'
version_id = 'latest'
bucket_name = 'ba882_team7'
today = datetime.today().strftime('%Y-%m-%d')

####################################################### helpers

def upload_to_gcs(bucket_name, job_id, df, file_name):
    """Uploads data to a Google Cloud Storage bucket as parquet."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    df_name = f'{df=}'.split('=')[0]
    blob_name = f"jobs/{job_id}/{file_name}.parquet"
    blob = bucket.blob(blob_name)

    # Convert DataFrame to parquet format in memory
    parquet_buffer = BytesIO()
    df.to_parquet(parquet_buffer, engine='pyarrow')
    parquet_buffer.seek(0)

    # Upload the parquet file
    blob.upload_from_file(parquet_buffer, content_type='application/octet-stream')
    print(f"Parquet file {blob_name} uploaded to {bucket_name}.")


####################################################### core task

@functions_framework.http
def task(request):

  ## job_id
  job_id = datetime.now().strftime("%Y%m%d%H%M") + "-" + str(uuid.uuid4())
  file_counter = 0

  ## instantiate services
  storage_client = storage.Client()

  ## Top 10 US Stocks by market cap
  stocks = ['AAPL', 'NVDA', 'MSFT', 'GOOG', 'AMZN', 'META', 'BRK-B', 'LLY', 'AVGO', 'TSLA']
  stocks_str = ' '.join(stocks) 
  tickers = yf.Tickers(stocks_str)

  ## Setting range of data
  backfill = True
  least_period = False

  if backfill == True:
    period = '1y'

  if backfill == False:
    if least_period == True:
      period = '5d'
    else:
      period = '1mo'

  ## Extracting Company Info (Latest)
  df_list = []

  for ticker in stocks:
    df_list.append(pd.DataFrame([tickers.tickers[ticker].info]))

  df_info = pd.concat(df_list)
  df_info['extraction_date'] = today
  upload_to_gcs(bucket_name, job_id, df_info, file_name='info') # upload file 
  file_counter += 1

  ## get historical price data 
  df_price = yf.download(stocks, group_by='Ticker', period=period)
  df_price = df_price.stack(level=0).rename_axis(['Date', 'Ticker']).reset_index() # Flatten dataframe

  upload_to_gcs(bucket_name, job_id, df_price, file_name='price') # upload file 
  file_counter += 1

  ## Extracting Actions Dividends/Stock Splits
  df_list = []

  for ticker in stocks:
    temp_df = tickers.tickers[ticker].actions
    temp_df['Ticker'] = ticker
    df_list.append(temp_df)

  df_actions = pd.concat(df_list)
  upload_to_gcs(bucket_name, job_id, df_actions, file_name='actions') # upload file 
  file_counter += 1


  ## Extract targets/forecasts :
  df_list = []

  for ticker in stocks:
    tick = tickers.tickers[ticker]
    temp_df = pd.DataFrame.from_dict(tick.calendar)
    temp_df['Ticker'] = ticker
    df_list.append(temp_df.head(1)) # only take first calendar

  df_calendar = pd.concat(df_list)
  df_calendar['extraction_date'] = today
  upload_to_gcs(bucket_name, job_id, df_calendar, file_name='calendar') # upload file 
  file_counter += 1


  ## SEC Filings
  df_list = []

  for ticker in stocks:
    if ticker == 'BRK-B':
        tick = yf.Ticker('BRK-A') # this class has SEC filings available
    else:
      tick = tickers.tickers[ticker] 
    temp_df = pd.DataFrame.from_dict(tick.sec_filings)
    temp_df['Ticker'] = ticker
    df_list.append(temp_df)

  df_sec = pd.concat(df_list)
  upload_to_gcs(bucket_name, job_id, df_sec, file_name='sec') # upload file 
  file_counter += 1


  # News Articles
  df_list = []

  for ticker in stocks:

    tick = tickers.tickers[ticker] 
    temp_df = pd.DataFrame.from_dict(tick.news)
    temp_df['Ticker'] = ticker
    df_list.append(temp_df)

  df_news = pd.concat(df_list)
  upload_to_gcs(bucket_name, job_id, df_news, file_name='news') # upload file 
  file_counter += 1
  
  return {
      "num_files": file_counter, 
      "job_id": job_id}, 200
