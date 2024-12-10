# imports
import functions_framework
from google.cloud import secretmanager
import duckdb

# setup
project_id = 'ba882-435919'
secret_id = 'motherduck_access_token'
version_id = '1'


# db setup
db = 'stocks'
raw_db_schema = f"{db}.raw"
stage_db_schema = f"{db}.stage"

############################################################### main task

@functions_framework.http
def test(request):
 
    # instantiate the services 
    sm = secretmanager.SecretManagerServiceClient()

    # Build the resource name of the secret version
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"

    # Access the secret version
    response = sm.access_secret_version(request={"name": name})
    md_token = response.payload.data.decode("UTF-8")

    # initiate the MotherDuck connection through an access token through
    md = duckdb.connect(f'md:?motherduck_token={md_token}') 

  
    print(md.sql("SHOW ALL TABLES;").show())


    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    tables = ['price', 'sec', 'news', 'actions', 'calendar', 'info']
    for table in tables:
      
      raw_tbl_name = f'stocks.raw.{table}'
      print(f"{raw_tbl_name}")
      print(md.sql(f"SELECT * FROM {raw_tbl_name} LIMIT 5;").show())
      print(md.sql(f"SELECT COUNT(*) FROM {raw_tbl_name};").show())

      stage_tbl_name = f'stocks.stage.{table}'
      print(f"{stage_tbl_name}")
      print(md.sql(f"SELECT * FROM {stage_tbl_name} LIMIT 5;").show())
      print(md.sql(f"SELECT COUNT(*) FROM {stage_tbl_name};").show())

    return {}, 200