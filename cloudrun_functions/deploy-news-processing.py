from prefect import flow

if __name__ == "__main__":
    flow.from_source(
        source="https://github.com/tangyum/BA-882-Pipeline-Project.git",
        entrypoint="cloudrun_functions/news_processing_flow.py:news_processing_flow",  # Adjust based on your main function
    ).deploy(
        name="cloud-functions-deployment",
        work_pool_name="shayan-lab6-pool1",
        job_variables={"env": {"Shayan": "value"}, "pip_packages": ["pandas", "requests"]},
        cron="0 22 * * *",  # Runs every hour, adjust as needed
        tags=["prod"],
        description="Deploys cloud functions to execute tasks on schedule.",
        version="1.0.0",
    )
