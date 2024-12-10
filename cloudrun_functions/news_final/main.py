# imports
import functions_framework
from google.cloud import secretmanager
import duckdb
import pandas as pd
import datetime
import re  

# setup
project_id = 'ba882-435919'
secret_id = 'motherduck_access_token'
version_id = '1'

# db setup
table_old = 'stocks.report.news_summarized'
table_new = 'stocks.report.news_final'
############################################################### main task

@functions_framework.http
def task(request):

    # instantiate the services
    sm = secretmanager.SecretManagerServiceClient()

    # Build the resource name of the secret version
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"

    # Access the secret version
    response = sm.access_secret_version(request={"name": name})
    md_token = response.payload.data.decode("UTF-8")

    # initiate the MotherDuck connection through an access token through
    md = duckdb.connect(f'md:?motherduck_token={md_token}')


    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ tbl: create new dataframe
    
    df = md.sql(f"""SELECT * FROM {table_old}""").df()
    
    # Convert date time
    df['datetime'] = df.apply(lambda row: datetime.datetime.fromtimestamp(row.providerPublishTime), axis=1)
    df = df.rename(columns={'summary':'summary_unparsed', 'sentiment':'sentiment_ml_score','bullets':'summary'})
    df['summary_unparsed'] = df['summary_unparsed'].apply(lambda sum: sum.replace('*',""))

    # Define the fields and their corresponding regex patterns
    field_patterns = {
        'summary': r'(?:summary:)(.*)\n',
        'sentiment_llm': r'(?:sentiment:)(.*)\n',
        'market_impact': r'(?:market impact:)(.*)(?:\n|$)'}

    def extract_field(text, pattern):
        try:
            match = re.search(pattern, text, re.IGNORECASE)
            return match.group(1).strip() if match else None
        except Exception as e:
            print(f"Error extracting field: {str(e)}")
            return None

    for field, pattern in field_patterns.items():
        df[field] = df['summary_unparsed'].apply(lambda x: extract_field(x, pattern))
        df[field] = df[field].str.strip()


    # to Motherduck
    md.sql(f'DROP TABLE IF EXISTS {table_new}')
    create_db_sql = f"CREATE TABLE IF NOT EXISTS {table_new} AS SELECT * FROM df;"
    md.sql(create_db_sql)

    print(md.sql(f'SELECT COUNT(*) FROM {table_new} records'))
    print(md.sql(f'SELECT * FROM {table_new} LIMIT 5'))

    return {'task':'complete'}, 200