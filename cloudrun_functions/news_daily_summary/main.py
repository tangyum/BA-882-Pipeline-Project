# imports
import functions_framework
import os
from google.cloud import secretmanager
import json
import duckdb
import pandas as pd
import vertexai
from vertexai.generative_models import GenerativeModel
import time
import requests

############################################################### main task

@functions_framework.http
def summarize_news(request):
    """
    HTTP Cloud Function to summarize news articles by ticker.
    """
    try:
        # Setup configurations
        project_id = 'ba882-435919'
        secret_id = 'motherduck_access_token'
        version_id = '1'
        db = 'stocks'
        reporting_db_schema = f"{db}.report"

        # Initialize services
        sm = secretmanager.SecretManagerServiceClient()
      

        # Get MotherDuck token
        name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
        response = sm.access_secret_version(request={"name": name})
        md_token = response.payload.data.decode("UTF-8")

        # Connect to MotherDuck
        md = duckdb.connect(f'md:?motherduck_token={md_token}')

        # Query data
        query = """
        SELECT *
        FROM stocks.report.news_final;
        """
        
        # Execute query and store results in DataFrame
        df = md.sql(query).df()
        df = df[df['summary_unparsed'] != 'Not Found']

        # Filter recent data
        today = pd.Timestamp.today(tz='est').date() - pd.to_timedelta(3, unit='d')

        df = df[df['datetime'].dt.date > today]

        # Group by ticker
        ticker_groups = df.groupby('Ticker')

        # Initialize Vertex AI
        vertexai.init(project=project_id, location='us-central1')
        model = GenerativeModel("gemini-1.5-pro-001")

        # Generate summaries
        summaries = {}
        for ticker, group in ticker_groups:
            time.sleep(15)
            articles = group['title'].str.cat(group['summary_unparsed'], sep=' ')
            articles = ('\n\n').join(list(articles))
            print('ticker')
            print('articles')

            prompt = f"""Summarize the key points from these news articles about {ticker}:
            {articles}
            Provide a comprehensive summary in 2-3 paragraphs."""

            response = model.generate_content(
                prompt,
                generation_config={
                    "temperature": 0,
                    "max_output_tokens": 1024,
                    "candidate_count": 1
                }
            )
            summaries[ticker] = response.text
            print(response.text)

        return {
            'status': 'success',
            'summaries': summaries,
            'ticker_count': len(summaries)
        }

    except Exception as e:
        return {
            'status': 'error',
            'error': str(e)
        }