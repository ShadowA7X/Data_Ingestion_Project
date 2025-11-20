# api_server.py

from fastapi import FastAPI, BackgroundTasks # for background task support
from ingestion.job import run_ingestion_job # import the ingestion job function

app = FastAPI( # API metadata
    title="Random User Ingestion Service", 
    description="API to trigger RandomUser ingestion jobs.",
    version="0.1.0", # version number. This number is arbitrary and can be changed as needed.
)


@app.get("/health") # Health check endpoint. Good for load balancers / uptime monitors. When called, it should return 200 OK if the service is running.
def health():
    return {"status": "ok"}


@app.post("/jobs/ingestion")
def trigger_ingestion(background_tasks: BackgroundTasks): # good for real users / UI / automation -> “fire and forget”
    """
    Trigger one ingestion run in the background. 

    Returns immediately with 'queued', while the job runs.
    """
    background_tasks.add_task(run_ingestion_job) #background task to run the ingestion job
    return {"status": "queued"} # immediate response


@app.post("/jobs/ingestion/sync") # good for debugging / scripts / monitoring -> “run now and give me the result”
def run_ingestion_sync():
    """
    Run the ingestion synchronously and return metrics.
    Handy for testing.
    """
    metrics = run_ingestion_job() # run the ingestion job and get metrics
    return {"status": "completed", "metrics": metrics} # This is triggered when the job is done


@app.get("/hello") # simple test endpoint to verify the service is running
def hello(): 
    return {"message": "RandomUser ingestion service is running"}
