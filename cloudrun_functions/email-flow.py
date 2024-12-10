from prefect import flow, task
import requests
import json

# Helper function to invoke GCF
def invoke_gcf(url: str, payload: dict):
    try:
        response = requests.post(url, json=payload)
        response.raise_for_status()
        print(f"Raw response from {url}: {response.text}")  # Debug log
        return response.json()
    except json.JSONDecodeError:
        print(f"Non-JSON response from {url}: {response.text}")
        return response.text  # Return plain text if not JSON
    except requests.exceptions.RequestException as e:
        print(f"Error invoking GCF at {url}: {e}")
        raise

@task(log_prints=True, retries=2)
def news_daily_report():
    """
    Task to generate a daily summary file for stocks.
    """
    url = "https://us-central1-ba882-435919.cloudfunctions.net/news_daily_report"
    payload = {}  # Adjust payload if necessary
    resp = invoke_gcf(url, payload=payload)
    print(f"Daily news summary result: {resp}")
    return resp

@task(log_prints=True, retries=2)
def podcast_and_email(summary_result):
    """
    Task to create a podcast and send it via email.
    """
    url = "https://us-central1-ba882-435919.cloudfunctions.net/podcast_and_email"
    payload = {"summary_result": summary_result}
    resp = invoke_gcf(url, payload=payload)
    print(f"Podcast and email result: {resp}")
    return resp

@flow(name="Daily-Podcast-and-Email-Flow", log_prints=True)
def daily_podcast_email_flow():
    """
    Orchestrates the daily summary generation, podcast creation, and email delivery.
    """
    # Step 1: Generate daily news summary
    summary_result = news_daily_report()
    print(f"News has been summarized: {summary_result}")

    # # Step 2: Create podcast and send email
    email_result = podcast_and_email(summary_result)
    print(f"{email_result}")

    print("Flow completed successfully!")
    # return email_result

if __name__ == "__main__":
    daily_podcast_email_flow()
