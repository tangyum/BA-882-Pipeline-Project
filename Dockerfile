# Use the official Python image.
FROM python:3.9-slim

# Set the working directory in the container.
WORKDIR /app

# Copy the current directory contents into the container at /app.
COPY . /app

# Install the required packages.
RUN pip install -r requirements.txt

# Make port 8080 available to the world outside this container.
EXPOSE 8080

# Define environment variable.
ENV PORT 8080

# Run app.py when the container launches.
CMD ["python", "app.py"]