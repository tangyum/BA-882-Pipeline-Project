gcloud functions deploy sentiment-analysis \
    --gen2 \
    --runtime python39 \
    --trigger-http \
    --entry-point task \
    --region us-central1 \
    --allow-unauthenticated \
    --memory 4GB \
    --timeout 540s
