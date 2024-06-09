# Use the official Python image from the Docker Hub
FROM python:3.9-slim

# Set the working directory
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Make port 8080 available to the world outside this container
ENV PORT 8080

# Define environment variable
ENV FLASK_APP main.py

# Run the Flask app
CMD ["flask", "run", "--host=0.0.0.0", "--port=8080"]
