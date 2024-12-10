# imports
import functions_framework
from google.cloud import secretmanager
import duckdb
import pandas as pd
from sec_api import RenderApi
import re
from google.cloud import storage


# setup

project_id = 'ba882-435919'
secret_id = 'motherduck_access_token'
version_id = '1'

# db setup
db = 'stocks'
reporting_db_schema = f"{db}.report"
created_tables = {}

def upload_blob(bucket_name='ba882_team7', source_file_name=file_name, excel_bytes=excel_content, destination_blob_name='ba882_team7/sec/xlsx'):
    """Uploads a file to the bucket."""
    # The ID of your GCS bucket
    # bucket_name = "your-bucket-name"
    # The path to your file to upload
    # source_file_name = "local/path/to/file"
    # The ID of your GCS object
    # destination_blob_name = "storage-object-name"

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name+f'/{source_file_name}')
    blob.upload_from_string(excel_bytes, content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')


    # Optional: set a generation-match precondition to avoid potential race conditions
    # and data corruptions. The request to upload is aborted if the object's
    # generation number does not match your precondition. For a destination
    # object that does not yet exist, set the if_generation_match precondition to 0.
    # If the destination object already exists in your bucket, set instead a
    # generation-match precondition using its generation number.


    print(
        f"File {source_file_name} uploaded to {destination_blob_name}."
    )


############################################################### main task

# instantiate the services
sm = secretmanager.SecretManagerServiceClient()

# Build the resource name of the secret version
name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"

# Access the secret version
response = sm.access_secret_version(request={"name": name})
md_token = response.payload.data.decode("UTF-8")

# initiate the MotherDuck connection through an access token through
md = duckdb.connect(f'md:?motherduck_token={md_token}')

@functions_framework.http
def task(request):
    sec_all = md.sql("SELECT * FROM stocks.stage.sec").df()
    financial_reports = ['10-K', '10-Q']
    sec_all = sec_all[sec_all['type'].isin(financial_reports)].reset_index(drop=True)
    sec_all['reportUrl'] = None
    sec_all['excelUrl'] = None

    for i, row in enumerate(sec_all.values):
        exhibits = json.loads(row[5].replace("'", '"').replace('None', '"None"')) # loading str as dict, replacing values to conform with Json format
        report = row[2]
        sec_all.iloc[i,-2] = exhibits.get(report)
        sec_all.iloc[i,-1] = exhibits.get('EXCEL')

    md.sql('DROP TABLE IF EXISTS stocks.report.sec_urls')
    md.sql('CREATE TABLE stocks.report.sec_urls AS SELECT * FROM sec_all')

    df = sec_all

    # download excel
    for i, row in enumerate(df.values):
        try:
            url = row[-2]
            code = re.search('(\d+\D\d+)', url).group()
            file_name = f'{row[-4]}_{row[2]}_{str(row[0])}.xlsx'
            url_excel_file    = f"https://www.sec.gov/Archives/edgar/data/{code}/Financial_Report.xlsx"
            # Download the Excel file
            excel_content = renderApi.get_filing(url_excel_file, return_binary=True)
            upload_blob(source_file_name=file_name, destination_blob_name='ba882_team7/sec/xlsx')


    return {'task':'completed'}
