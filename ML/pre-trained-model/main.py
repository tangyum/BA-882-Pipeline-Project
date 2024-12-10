import functions_framework
from transformers import pipeline
import logging
import numpy as np

# Load the sentiment analysis pipeline from Hugging Face
try:
    sentiment_pipeline = pipeline("sentiment-analysis")
    logging.info("Pretrained Hugging Face sentiment model loaded successfully")
except Exception as e:
    logging.error(f"Failed to load Hugging Face model: {e}")
    sentiment_pipeline = None

@functions_framework.http
def task(request):
    """
    Make predictions using the pretrained Hugging Face sentiment analysis model.
    """
    if sentiment_pipeline is None:
        return {'error': 'Model failed to load. Check logs for details.'}, 500

    # Parse the request data
    request_json = request.get_json(silent=True)
    if not request_json or 'data' not in request_json:
        logging.error("Invalid request format: 'data' key is missing")
        return {'error': "Invalid request format. JSON with 'data' key is required."}, 400

    # Load the 'data' key which is a list of text for inference
    data_list = request_json.get('data')
    logging.info(f"Data received for prediction: {data_list}")

    if not isinstance(data_list, list):
        logging.error("Invalid data type: 'data' should be a list of texts")
        return {'error': "Invalid data type. 'data' should be a list of texts."}, 400

    # Make predictions using the Hugging Face pipeline
    try:
        predictions = sentiment_pipeline(data_list)
        response = [{"text": text, "label": pred['label'], "score": pred['score']} for text, pred in zip(data_list, predictions)]
    except Exception as e:
        logging.error(f"Error during prediction: {e}")
        return {'error': 'Failed to make predictions. Check logs for details.'}, 500

    # Return predictions
    return {'predictions': response}, 200
