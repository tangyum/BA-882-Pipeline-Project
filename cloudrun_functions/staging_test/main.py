# imports
import functions_framework
from google.cloud import secretmanager
from google.cloud import storage
import json
import duckdb
import pandas as pd

# setup
project_id = 'ba882-435919'
secret_id = 'motherduck_access_token'
version_id = '1'

# db setup
db = 'stocks'
raw_db_schema = f"{db}.raw"
stage_db_schema = f"{db}.stage"
stage_tables = {}

############################################################### main task

@functions_framework.http
def task(request):

    # Parse the request data
    request_json = request.get_json(silent=True)
    print(f"request: {json.dumps(request_json)}")

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

    # create db if not exists
    create_db_sql = f"CREATE DATABASE IF NOT EXISTS {db};"
    md.sql(create_db_sql)
    # print(md.sql("SHOW DATABASES").show())

    # drop if exists and create the raw schema for
    create_schema = f"DROP SCHEMA IF EXISTS {raw_db_schema} CASCADE; CREATE SCHEMA IF NOT EXISTS {raw_db_schema};"
    md.sql(create_schema)

    # create stage schema if first time running function
    create_schema = f"CREATE SCHEMA IF NOT EXISTS {stage_db_schema};"
    md.sql(create_schema)

    print(md.sql("SHOW DATABASES;").show())


    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ tbl: price

    # read in from gcs
    parquet_path = request_json.get('price')
    parquet_df = pd.read_parquet(parquet_path)

    # table logic
    tbl_name = 'price'
    idx_name = tbl_name+'_idx'
    raw_tbl_name = f"{raw_db_schema}.{tbl_name}"
    stage_tbl_name = f"{stage_db_schema}.{tbl_name}"

    # load from df
    raw_tbl_sql = f"""
    DROP TABLE IF EXISTS {raw_tbl_name} ;
    CREATE TABLE {raw_tbl_name} AS SELECT * FROM parquet_df;
    """
    print(f"{raw_tbl_sql}")
    md.sql(raw_tbl_sql)
    del parquet_df

    # Create unique idx - Date + Ticker
    idx_create_sql = f"""CREATE TABLE {raw_tbl_name+'_idx'} AS SELECT *, (Date::TEXT || Ticker) AS {idx_name} FROM {raw_tbl_name};"""
    md.sql(idx_create_sql)
    raw_tbl_name = raw_tbl_name+'_idx'

    print(f"{raw_tbl_name}")
    print(md.sql(f"SELECT * FROM {raw_tbl_name} LIMIT 5;").show())
    print(md.sql(f"SELECT COUNT(*) FROM {raw_tbl_name};").show())


    # create staging table if first time running function
    create_stage = f"""CREATE TABLE IF NOT EXISTS {stage_tbl_name} AS SELECT * from {raw_tbl_name}"""
    print(create_stage)
    md.sql(create_stage)

    # upsert like operation -- will only insert new records, not update

    upsert_sql = f"""
    INSERT INTO {stage_tbl_name}
    SELECT raw.*
    FROM {raw_tbl_name} AS raw
    ANTI JOIN {stage_tbl_name} AS stage
    ON stage.{idx_name} = raw.{idx_name}"""

    print(upsert_sql)
    md.sql(upsert_sql)

    print(f"{stage_tbl_name}")
    print(md.sql(f"SELECT * FROM {stage_tbl_name} LIMIT 5;").show())
    print(md.sql(f"SELECT COUNT(*) FROM {stage_tbl_name};").show())
    stage_tables.update({f'{tbl_name}':f'{stage_tbl_name}'})

        #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ tbl: actions

    # read in from gcs
    parquet_path = request_json.get('actions')
    parquet_df = pd.read_parquet(parquet_path)
    parquet_df = parquet_df.reset_index()

    # table logic
    tbl_name = 'actions'
    idx_name = tbl_name+'_idx'
    raw_tbl_name = f"{raw_db_schema}.{tbl_name}"
    stage_tbl_name = f"{stage_db_schema}.{tbl_name}"

    # load from df
    raw_tbl_sql = f"""
    DROP TABLE IF EXISTS {raw_tbl_name} ;
    CREATE TABLE {raw_tbl_name} AS SELECT * FROM parquet_df;
    """
    print(f"{raw_tbl_sql}")
    md.sql(raw_tbl_sql)
    del parquet_df

    print(f'{raw_tbl_name} - pre index')
    print(md.sql(f"SELECT * FROM {raw_tbl_name} LIMIT 5;").show())

    # Create unique idx - Date + Ticker
    idx_create_sql = f"""CREATE TABLE {raw_tbl_name+'_idx'} AS SELECT *, (Date::TEXT || Ticker) AS {idx_name} FROM {raw_tbl_name};"""
    md.sql(idx_create_sql)
    raw_tbl_name = raw_tbl_name+'_idx'

    print(f"{raw_tbl_name}")
    print(md.sql(f"SELECT * FROM {raw_tbl_name} LIMIT 5;").show())
    print(md.sql(f"SELECT COUNT(*) FROM {raw_tbl_name};").show())


    # create staging table if first time running function
    create_stage = f"""CREATE TABLE IF NOT EXISTS {stage_tbl_name} AS SELECT * from {raw_tbl_name}"""
    print(create_stage)
    md.sql(create_stage)

    # upsert like operation -- will only insert new records, not update

    upsert_sql = f"""
    INSERT INTO {stage_tbl_name}
    SELECT raw.*
    FROM {raw_tbl_name} AS raw
    ANTI JOIN {stage_tbl_name} AS stage
    ON stage.{idx_name} = raw.{idx_name}"""

    print(upsert_sql)
    md.sql(upsert_sql)

    print(f"{stage_tbl_name}")
    print(md.sql(f"SELECT * FROM {stage_tbl_name} LIMIT 5;").show())
    print(md.sql(f"SELECT COUNT(*) FROM {stage_tbl_name};").show())
    stage_tables.update({f'{tbl_name}':f'{stage_tbl_name}'})

        #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ tbl: calendar

    # read in from gcs
    parquet_path = request_json.get('calendar')
    parquet_df = pd.read_parquet(parquet_path)

    # table logic
    tbl_name = 'calendar'
    idx_name = tbl_name+'_idx'
    raw_tbl_name = f"{raw_db_schema}.{tbl_name}"
    stage_tbl_name = f"{stage_db_schema}.{tbl_name}"

    # load from df
    raw_tbl_sql = f"""
    DROP TABLE IF EXISTS {raw_tbl_name} ;
    CREATE TABLE {raw_tbl_name} AS SELECT * FROM parquet_df;
    """
    print(f"{raw_tbl_sql}")
    md.sql(raw_tbl_sql)
    del parquet_df

    # Create unique idx - Date + Ticker
    idx_create_sql = f"""CREATE TABLE {raw_tbl_name+'_idx'} AS SELECT *, (extraction_date::TEXT || Ticker) AS {idx_name} FROM {raw_tbl_name};"""
    md.sql(idx_create_sql)
    raw_tbl_name = raw_tbl_name+'_idx'

    print(f"{raw_tbl_name}")
    print(md.sql(f"SELECT * FROM {raw_tbl_name} LIMIT 5;").show())
    print(md.sql(f"SELECT COUNT(*) FROM {raw_tbl_name};").show())


    # create staging table if first time running function
    create_stage = f"""CREATE TABLE IF NOT EXISTS {stage_tbl_name} AS SELECT * from {raw_tbl_name}"""
    print(create_stage)
    md.sql(create_stage)

    # upsert like operation -- will only insert new records, not update

    upsert_sql = f"""
    INSERT INTO {stage_tbl_name}
    SELECT raw.*
    FROM {raw_tbl_name} AS raw
    ANTI JOIN {stage_tbl_name} AS stage
    ON stage.{idx_name} = raw.{idx_name}"""

    print(upsert_sql)
    md.sql(upsert_sql)

    print(f"{stage_tbl_name}")
    print(md.sql(f"SELECT * FROM {stage_tbl_name} LIMIT 5;").show())
    print(md.sql(f"SELECT COUNT(*) FROM {stage_tbl_name};").show())
    stage_tables.update({f'{tbl_name}':f'{stage_tbl_name}'})

        #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ tbl: sec

    # read in from gcs
    parquet_path = request_json.get('sec')
    parquet_df = pd.read_parquet(parquet_path)
    parquet_df['exhibits'] = parquet_df['exhibits'].astype('str') # convert struct format to string (this will allow upsert without conflict on struct format)

    # table logic
    tbl_name = 'sec'
    idx_name = tbl_name+'_idx'
    raw_tbl_name = f"{raw_db_schema}.{tbl_name}"
    stage_tbl_name = f"{stage_db_schema}.{tbl_name}"

    # load from df
    raw_tbl_sql = f"""
    DROP TABLE IF EXISTS {raw_tbl_name} ;
    CREATE TABLE {raw_tbl_name} AS SELECT * FROM parquet_df;
    """
    print(f"{raw_tbl_sql}")
    md.sql(raw_tbl_sql)
    del parquet_df

    # Create unique idx - Date + Ticker
    idx_create_sql = f"""CREATE TABLE {raw_tbl_name+'_idx'} AS SELECT *, (epochDate::TEXT || Ticker) AS {idx_name} FROM {raw_tbl_name};"""
    md.sql(idx_create_sql)
    raw_tbl_name = raw_tbl_name+'_idx'

    print(f"{raw_tbl_name}")
    print(md.sql(f"SELECT * FROM {raw_tbl_name} LIMIT 5;").show())
    print(md.sql(f"SELECT COUNT(*) FROM {raw_tbl_name};").show())


    # create staging table if first time running function
    create_stage = f"""CREATE TABLE IF NOT EXISTS {stage_tbl_name} AS SELECT * from {raw_tbl_name}"""
    print(create_stage)
    md.sql(create_stage)

    # upsert like operation -- will only insert new records, not update

    upsert_sql = f"""
    INSERT INTO {stage_tbl_name}
    SELECT raw.*
    FROM {raw_tbl_name} AS raw
    ANTI JOIN {stage_tbl_name} AS stage
    ON stage.{idx_name} = raw.{idx_name}"""

    print(upsert_sql)
    md.sql(upsert_sql)

    print(f"{stage_tbl_name}")
    print(md.sql(f"SELECT * FROM {stage_tbl_name} LIMIT 5;").show())
    print(md.sql(f"SELECT COUNT(*) FROM {stage_tbl_name};").show())
    stage_tables.update({f'{tbl_name}':f'{stage_tbl_name}'})
        #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ tbl: news

    # read in from gcs
    parquet_path = request_json.get('news')
    parquet_df = pd.read_parquet(parquet_path)

    # table logic
    tbl_name = 'news'
    idx_name = tbl_name+'_idx'
    raw_tbl_name = f"{raw_db_schema}.{tbl_name}"
    stage_tbl_name = f"{stage_db_schema}.{tbl_name}"

    # load from df
    raw_tbl_sql = f"""
    DROP TABLE IF EXISTS {raw_tbl_name} ;
    CREATE TABLE {raw_tbl_name} AS SELECT * FROM parquet_df;
    """
    print(f"{raw_tbl_sql}")
    md.sql(raw_tbl_sql)
    del parquet_df

    # # Create unique idx - Date + Ticker
    # idx_create_sql = f"""CREATE TABLE {raw_tbl_name+'_idx'} AS SELECT *, (Date::TEXT || Ticker) AS {idx_name} FROM {raw_tbl_name};"""
    # md.sql(idx_create_sql)
    # raw_tbl_name = raw_tbl_name+'_idx'
    idx_name = 'uuid'

    print(f"{raw_tbl_name}")
    print(md.sql(f"SELECT * FROM {raw_tbl_name} LIMIT 5;").show())
    print(md.sql(f"SELECT COUNT(*) FROM {raw_tbl_name};").show())


    # create staging table if first time running function
    create_stage = f"""CREATE TABLE IF NOT EXISTS {stage_tbl_name} AS SELECT * from {raw_tbl_name}"""
    print(create_stage)
    md.sql(create_stage)

    # upsert like operation -- will only insert new records, not update

    upsert_sql = f"""
    INSERT INTO {stage_tbl_name}
    SELECT raw.*
    FROM {raw_tbl_name} AS raw
    ANTI JOIN {stage_tbl_name} AS stage
    ON stage.{idx_name} = raw.{idx_name}"""

    print(upsert_sql)
    md.sql(upsert_sql)

    print(f"{stage_tbl_name}")
    print(md.sql(f"SELECT * FROM {stage_tbl_name} LIMIT 5;").show())
    print(md.sql(f"SELECT COUNT(*) FROM {stage_tbl_name};").show())
    stage_tables.update({f'{tbl_name}':f'{stage_tbl_name}'})

        #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ tbl: info

    # read in from gcs
    parquet_path = request_json.get('info')
    parquet_df = pd.read_parquet(parquet_path)

    # table logic
    tbl_name = 'info'
    idx_name = tbl_name+'_idx'
    raw_tbl_name = f"{raw_db_schema}.{tbl_name}"
    stage_tbl_name = f"{stage_db_schema}.{tbl_name}"

    # load from df
    raw_tbl_sql = f"""
    DROP TABLE IF EXISTS {raw_tbl_name} ;
    CREATE TABLE {raw_tbl_name} AS SELECT * FROM parquet_df;
    """
    print(f"{raw_tbl_sql}")
    md.sql(raw_tbl_sql)
    del parquet_df

    # # Create unique idx - Date + Ticker
    # idx_create_sql = f"""CREATE TABLE {raw_tbl_name+'_idx'} AS SELECT *, (Date::TEXT || Ticker) AS {idx_name} FROM {raw_tbl_name};"""
    # md.sql(idx_create_sql)
    # raw_tbl_name = raw_tbl_name+'_idx'
    idx_name = 'uuid'

    print(f"{raw_tbl_name}")
    print(md.sql(f"SELECT * FROM {raw_tbl_name} LIMIT 5;").show())
    print(md.sql(f"SELECT COUNT(*) FROM {raw_tbl_name};").show())


    # create staging table if first time running function
    create_stage = f"""CREATE TABLE IF NOT EXISTS {stage_tbl_name} AS SELECT * from {raw_tbl_name}"""
    print(create_stage)
    md.sql(create_stage)

    # upsert like operation -- will only insert new records, not update

    upsert_sql = f"""
    INSERT INTO {stage_tbl_name}
    SELECT raw.*
    FROM {raw_tbl_name} AS raw
    ANTI JOIN {stage_tbl_name} AS stage
    ON stage.{idx_name} = raw.{idx_name}"""

    print(upsert_sql)
    md.sql(upsert_sql)

    print(f"{stage_tbl_name}")
    print(md.sql(f"SELECT * FROM {stage_tbl_name} LIMIT 5;").show())
    print(md.sql(f"SELECT COUNT(*) FROM {stage_tbl_name};").show())
    stage_tables.update({f'{tbl_name}':f'{stage_tbl_name}'})

    return {'staging_tables':stage_tables}, 200