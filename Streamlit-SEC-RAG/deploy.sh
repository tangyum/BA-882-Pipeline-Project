# setup the project
gcloud auth login
gcloud config set project ba882-435919

echo "======================================================"
echo "build (no cache)"
echo "======================================================"

docker build -t us-central1-docker.pkg.dev/ba882-435919/sec-rag/streamlit-rag-app . #--no-cache

echo "======================================================"
echo "push"
echo "======================================================"

docker push us-central1-docker.pkg.dev/ba882-435919/sec-rag/streamlit-rag-app

echo "======================================================"
echo "deploy run"
echo "======================================================"


gcloud run deploy streamlit-rag-app \
    --image us-central1-docker.pkg.dev/ba882-435919/sec-rag/streamlit-rag-app \
    --platform managed \
    --region us-central1 \
    --allow-unauthenticated \
    --memory 4Gi