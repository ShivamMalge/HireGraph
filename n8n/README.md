# HireGraph n8n Setup

## Workflows

- `n8n/workflows/hiregraph_pipeline.json`
- `n8n/workflows/hiregraph_error_workflow.json`

## Import Order

1. Import `hiregraph_error_workflow.json`
2. Import `hiregraph_pipeline.json`
3. In `HireGraph Automated Ingestion`, confirm the error workflow points to `HireGraph Pipeline Errors`

## Main Workflow Behavior

1. Runs every 30 minutes via Cron
2. Calls the scraper service at `http://host.docker.internal:5000/scrape`
3. Normalizes scraper output into `{ url, raw_payload }`
4. Splits jobs into batches of 1
5. Sends each job to `http://backend:8000/raw-jobs`
6. Extracts `raw_ingestion_id`
7. Calls `http://backend:8000/process-raw/{id}`
8. Loops until the batch is complete

## Scraper Response Contract

The scraper endpoint should return either:

```json
[
  {
    "url": "https://wellfound.com/jobs/example",
    "raw_payload": "Title: Backend Engineer\nDescription: ..."
  }
]
```

or:

```json
{
  "jobs": [
    {
      "url": "https://wellfound.com/jobs/example",
      "raw_payload": "Title: Backend Engineer\nDescription: ..."
    }
  ]
}
```

## Networking

- Backend is called through Docker DNS: `http://backend:8000`
- Scraper is called through host networking: `http://host.docker.internal:5000/scrape`

## Error Handling

- HTTP nodes retry up to 3 times
- Failures flow into `HireGraph Pipeline Errors`
- Error workflow writes `[ORCHESTRATOR]` messages to the n8n execution log

## Optional Manual Trigger

The main workflow includes a webhook node at:

- `POST /webhook/hiregraph/manual-ingestion`

This accepts the same scraper-style payload and pushes jobs into the pipeline without waiting for the next cron run.
