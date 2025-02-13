import functions_framework
from google.cloud import secretmanager
import duckdb
import requests  # Ensure you import requests

# settings
project_id = 'ba882-435919'
secret_id = 'motherduck_access_token'  # this is the name of the secret you created
version_id = 'latest'

# db setup
db = 'stocks'
schema = "report"
db_schema = f"{db}.{schema}"


@functions_framework.http
def task(request):
    # Instantiate the services
    sm = secretmanager.SecretManagerServiceClient()

    # Build the resource name of the secret version
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"

    # Access the secret version
    response = sm.access_secret_version(request={"name": name})
    md_token = response.payload.data.decode("UTF-8")

    # Initiate the MotherDuck connection through an access token
    md = duckdb.connect(f'md:?motherduck_token={md_token}')

    # Fetch data from MotherDuck (excluding sentiment column)
    query = """
        SELECT uuid, title, publisher, link, providerPublishTime, type, thumbnail, 
               relatedTickers, Ticker, scraped_text, bullets, summary, sentiment
        FROM stocks.report.news_summarized
    """
    data = md.execute(query).fetchall()

    # Cloud Function URL for sentiment analysis
    CLOUD_FUNCTION_URL = "https://us-central1-ba882-435919.cloudfunctions.net/sentiment-analysis"

    # Function to call the Cloud Function and analyze sentiment
    def analyze_sentiment(summaries):
        payload = {"data": summaries}
        response = requests.post(CLOUD_FUNCTION_URL, json=payload)
        if response.status_code == 200:
            return response.json().get("predictions", [])
        else:
            raise Exception(f"Error calling Cloud Function: {response.text}")

    # Predict sentiments for summaries in batches
    batch_size = 50
    summaries = [row[-1] for row in data]  # Extract summaries (last column in fetched data)
    sentiments = []

    for i in range(0, len(summaries), batch_size):
        batch = summaries[i:i + batch_size]
        sentiments.extend(analyze_sentiment(batch))

    # Check if table exists (Optional)
    check_table_sql = f"""
        SELECT COUNT(*) 
        FROM information_schema.tables 
        WHERE table_schema = '{schema}' AND table_name = 'news_with_sentiment'
    """
    table_exists = md.execute(check_table_sql).fetchone()[0] > 0

    if not table_exists:
        create_table_sql = f"""
            CREATE TABLE {db_schema}.news_with_sentiment AS 
            SELECT *, '' AS sentiment
            FROM {db_schema}.news_summarized
            WHERE 1=0  -- Creates the structure without copying data
        """
        md.execute(create_table_sql)

    # Insert data into the existing table
    rows_with_sentiments = [
        (*row, sentiment) for row, sentiment in zip(data, sentiments)
    ]
    insert_sql = f"""
        INSERT INTO {db_schema}.news_with_sentiment 
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    """
    md.executemany(insert_sql, rows_with_sentiments)

    print("Sentiment analysis table updated successfully.")

    return "Success", 200
