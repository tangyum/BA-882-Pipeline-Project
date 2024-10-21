# The ETL job orchestrator

# imports
import requests
import json
from prefect import flow, task

# helper function - generic invoker
def invoke_gcf(url:str, payload:dict):
    response = requests.post(url, json=payload)
    response.raise_for_status()
    return response.json()


# @task(retries=2)
# def schema_setup_stock():
#     """Setup the stage schema"""
#     url = "https://us-central1-ba882-435919.cloudfunctions.net/dev-schema-setup_stock"
#     resp = invoke_gcf(url, payload={})
#     return resp

@task(retries=2)
def yfinance_dump():
    """Extract the RSS feeds into JSON on GCS"""
    url = "https://us-central1-ba882-435919.cloudfunctions.net/yfinance_dump"
    resp = invoke_gcf(url, payload={})
    return resp

@task(retries=2)
def staging(payload):
    """Process the RSS feed JSON into parquet on GCS"""
    url = "https://us-central1-ba882-435919.cloudfunctions.net/staging"
    resp = invoke_gcf(url, payload=payload)
    return resp

# @task(retries=2)
# def fetch_motherduck_info(payload):
#     """Load the tables into the raw schema, ingest new records into stage tables"""
#     url = "https://us-central1-ba882-435919.cloudfunctions.net/fetch_motherduck_info"
#     resp = invoke_gcf(url, payload=payload)
#     return resp

# Prefect Flow
@flow(name="882-project-flow", log_prints=True)
def etl_flow():
    """The ETL flow which orchestrates Cloud Functions"""

    # result = schema_setup_stock()
    # print("The schema setup completed")
    
    extract_result = yfinance_dump()
    print("The RSS feeds were extracted onto GCS")
    print(f"{extract_result}")
    
    transform_result = staging(extract_result)
    print("The parsing of the feeds into tables completed")
    print(f"{transform_result}")

    # result = fetch_motherduck_info(transform_result)
    # print("The data were loaded into the raw schema and changes added to stage")


# the job
if __name__ == "__main__":
    etl_flow()