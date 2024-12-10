from prefect import flow, task
import requests
import json

def invoke_gcf(url:str, payload:dict):
    try:
        response = requests.post(url, json=payload)
        response.raise_for_status()
        # print(f"Raw response from {url}: {response.text}")  # Debugging log
        try:
            return response.json()
        except json.JSONDecodeError:
            print(f"Non-JSON response from {url}: {response.text}")
            return response.text  # Return plain text
    except requests.exceptions.RequestException as e:
        print(f"Error invoking GCF at {url}: {e}")
        raise
    return response.json()


@task(retries=2)
def scrape_news():
    """Scrape the news articles and store in MotherDuck"""
    url = "https://us-central1-ba882-435919.cloudfunctions.net/scrape_news"
    resp = invoke_gcf(url, payload={})
    return resp

@task(retries=2)
def summarize_news(payload):
    """Summarize the news articles"""
    url = "https://us-central1-ba882-435919.cloudfunctions.net/summarize_news"
    # print(f"Payload sent to summarize_news: {payload}")  # Debug log
    resp = invoke_gcf(url, payload=payload)
    # print(f"Response from summarize_news: {resp}")  # Debug log
    return resp


@flow(name="news-processing-flow", log_prints=True)
def news_processing_flow():
    """A flow to scrape and summarize news articles"""

    # Step 1 Scrape the news

    scraped_data = scrape_news()
    print("News has been scraped successfully")
    print(f"{scraped_data}")

    # Step 2 Summarize the data
    summarized_data = summarize_news(scraped_data)
    print("News has been summarized successfully!")
    print(f"{summarized_data}")

if __name__ == "__main__":
    news_processing_flow()
