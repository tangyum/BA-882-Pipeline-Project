import functions_framework
from google.cloud import secretmanager
import json
import duckdb
import pandas as pd
from openai import OpenAI
import os

# Function configuration
project_id = 'ba882-435919'  # Replace with your project ID
secret_id = 'motherduck_access_token'
secret_id_openai = 'openai_token'
version_id = '1'


@functions_framework.http  # Ensures Cloud Functions recognize this as HTTP
def summarize_news(request):
    """
    Summarizes scraped news articles from MotherDuck using OpenAI.
    """
    # Establish MotherDuck connection
    sm = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    response = sm.access_secret_version(request={"name": name})
    md_token = response.payload.data.decode("UTF-8")
    md = duckdb.connect(f'md:?motherduck_token={md_token}')

    # Database setup (optional, can be refactored)
    db = 'stocks'
    reporting_db_schema = f"{db}.report"
    tbl_name = 'news_story_unique'

    try:
        # Read scraped news data
        fetch_sql = f"SELECT * FROM {reporting_db_schema}.{tbl_name};"
        extracted_news = md.sql(fetch_sql).df()

        # Find unprocessed articles via anti-join
        fetch_existing_table = f"SELECT * FROM {reporting_db_schema}.news_summarized;"
        summaries = md.sql(fetch_existing_table).df()
        summaries = summaries[summaries['summary'] != 'Not Found']

        uuids = summaries['uuid'].to_list()
        df = extracted_news.loc[~extracted_news['uuid'].isin(uuids)]
        df['sentiment'] = ""


        # Summarize each article
        for i, row in df.iterrows():
            text = row.get('scraped_text')  # Adjust column name if needed
            if text and len(text) > 250: # filter out incomplete article stubs
                # OpenAI configuration
                name = f"projects/{project_id}/secrets/{secret_id_openai}/versions/{version_id}"
                response = sm.access_secret_version(request={"name": name})
                openai_token = response.payload.data.decode("UTF-8")

                os.environ['OPENAI_API_KEY'] = openai_token # Replace with your actual key
                client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))

                # Summarization prompt
                prompt = f"""You are a helpful summarizer. You are given a news article, and will respond in this format:

                Summary: A 100 words or less summary of the article
                Sentiment: The general sentiment of the article with respect to the company being discussed (Positive, Neutral or Negative)
                Market Impact: A short note on potential impact of this news on stock price

                Article: {text}"""

                # Send OpenAI request
                chat_completion = client.chat.completions.create(
                    messages=[{"role": "user", "content": prompt}],
                    model="gpt-4o",  # Adjust model as needed
                )
                summary = chat_completion.choices[0].message.content
                df.loc[i, 'summary'] = summary  # Update summary in DataFrame
                print(summary)

    except Exception as e:
        return f"Error summarizing news: {e}", 500

    # save summarized data back to the database here
    upsert_sql = f"""
                    INSERT INTO {reporting_db_schema}.news_summarized
                    SELECT * FROM df
                    ANTI JOIN {reporting_db_schema}.news_summarized AS stage
                    ON stage.uuid = df.uuid"""

    print(upsert_sql)
    md.sql(upsert_sql)

    return "Summarized news articles stored in MD", 200