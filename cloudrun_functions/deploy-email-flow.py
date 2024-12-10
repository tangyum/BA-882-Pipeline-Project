from prefect import flow

if __name__ == "__main__":
    flow.from_source(
        source="https://github.com/tangyum/BA-882-Pipeline-Project.git",
        entrypoint="cloudrun_functions/email-flow.py:daily_podcast_email_flow",  # Adjust based on your main function
    ).deploy(
        name="cloud-functions-deployment",
        work_pool_name="shayan-lab6-pool1",
        job_variables={"env": {"Shayan": "value"}, "pip_packages": ["pandas", "requests"]},
        cron="0 14 * * *",  # Runs every hour, adjust as needed
        tags=["prod"],
        description="Generate and run the email flow and send emails to people.",
        version="1.0.0",
    )
