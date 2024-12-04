# imports
import functions_framework
from google.cloud import secretmanager
from google.cloud import storage
import json
import duckdb
import pandas as pd
import requests
from bs4 import BeautifulSoup

# setup
project_id = 'ba882-435919'
secret_id = 'motherduck_access_token'
version_id = '1'

# db setup
db = 'stocks'
staging_db_schema = f"{db}.stage"
reporting_db_schema = f"{db}.report"
created_tables = {}

############################################################### main task
@functions_framework.http
def task(request):

    # instantiate the services
    sm = secretmanager.SecretManagerServiceClient()
    storage_client = storage.Client()

    # Build the resource name of the secret version
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"

    # Access the secret version
    response = sm.access_secret_version(request={"name": name})
    md_token = response.payload.data.decode("UTF-8")

    # initiate the MotherDuck connection through an access token through
    md = duckdb.connect(f'md:?motherduck_token={md_token}')

    # create stage schema if first time running function
    create_schema = f"CREATE SCHEMA IF NOT EXISTS {reporting_db_schema};"
    md.sql(create_schema)

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ tbl: news
    tbl_name = 'news_story_unique'
    # read in from md
    fetch_news = f"SELECT * FROM {staging_db_schema}.news;"
    df = md.sql(fetch_news).df()

    # Adding cols
    df['scraped_text'] = 'Not Found'
    df['bullets'] = 'Not Found'
    df['summary'] = 'Not Found'

    unique_uuid = df.drop_duplicates(subset=['uuid']) # dropping articles that are repeated for multiple tickers
    unique_uuid = unique_uuid[unique_uuid['type']=='STORY'] # scraper code only works for articles, not VIDEOS

    try: # filter out existing scraped articles
      fetch_existing_table = f"SELECT * FROM {reporting_db_schema}.{tbl_name};"
      anti_df = md.sql(fetch_existing_table).df()
      uuids = anti_df['uuid'].to_list()
      unique_uuid = unique_uuid.loc[~unique_uuid['uuid'].isin(uuids)]
    except: pass

    # Scraping

    for i, row in enumerate(unique_uuid.values):
      url = row[3]
      print(url)

      try:
        # Send a GET request to the URL
        response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
        # Parse the HTML content
        soup = BeautifulSoup(response.content, 'html.parser')

        # Extract the article content
        article_body = soup.find('div', class_='body yf-5ef8bf')
        paragraphs = article_body.find_all('p')
        content = '\n\n'.join([p.text for p in paragraphs])
        print(content)
        unique_uuid.iloc[i,-3] = content
      except Exception as error:
        print(str(error) + ' Content is likely behind paywall')
        unique_uuid.iloc[i,-3] = 'Paywall'

    # upsert from df

    upsert_sql = f"""
    INSERT INTO {reporting_db_schema}.{tbl_name}
    SELECT * FROM unique_uuid"""

    print(upsert_sql)
    md.sql(upsert_sql)


    return {'response':'200'}