# Use an official Python runtime as a parent image
FROM python:3.10

# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Install any needed packages specified in requirements.txt
RUN pip install -r requirements.txt #--no-cache-dir

# Expose port 8501 (Streamlit default)
EXPOSE 8501

# Streamlit specific command to enable access to the app
ENV STREAMLIT_SERVER_HEADLESS=true
ENV STREAMLIT_SERVER_ADDRESS=0.0.0.0

# Command to run the app using Streamlit
CMD ["streamlit", "run", "SEC-RAG-LATEST.py", "--server.port", "8080", "--server.address", "0.0.0.0"]