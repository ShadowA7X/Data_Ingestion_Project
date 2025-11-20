# api_server.py

from fastapi import FastAPI, BackgroundTasks
from ingestion.job import run_ingestion_job

app = FastAPI(
    title="Random User Ingestion Service",
    description="API to trigger RandomUser ingestion jobs.",
    version="0.1.0",
)


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/jobs/ingestion")
def trigger_ingestion(background_tasks: BackgroundTasks):
    """
    Trigger one ingestion run in the background.

    Returns immediately with 'queued', while the job runs.
    """
    background_tasks.add_task(run_ingestion_job)
    return {"status": "queued"}


@app.post("/jobs/ingestion/sync")
def run_ingestion_sync():
    """
    Run the ingestion synchronously and return metrics.
    Handy for testing.
    """
    metrics = run_ingestion_job()
    return {"status": "completed", "metrics": metrics}


@app.get("/hello")
def hello():
    return {"message": "RandomUser ingestion service is running"}
