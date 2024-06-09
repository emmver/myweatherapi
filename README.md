### README

## Weather Data Fetch and API Application

This repository contains two main components for handling weather data:
1. **data_fetch**: Contains scripts and configurations to fetch weather data from Meteomatics and load it into Google BigQuery.
2. **flask_api**: Contains a Flask application that provides an API to query the weather data stored in BigQuery.

### Repository Structure

```
.
├── data_fetch/
│   ├── main.py
│   ├── requirements.txt
│   └── README.md
├── flask_api/
│   ├── main.py
│   ├── requirements.txt
│   ├── Dockerfile
│   └── README.md
└── README.md
```

### Getting Started

#### Prerequisites

- Python 3.8+
- Google Cloud SDK
- Docker
- Git

#### Set Up Your Google Cloud Environment

1. **Enable APIs**:
   Ensure the following APIs are enabled in your Google Cloud project:
   - Cloud Run
   - Cloud Build
   - BigQuery
   - Secret Manager
   - Cloud Functions
   - Cloud Scheduler

   ```sh
   gcloud services enable run.googleapis.com
   gcloud services enable cloudbuild.googleapis.com
   gcloud services enable bigquery.googleapis.com
   gcloud services enable secretmanager.googleapis.com
   gcloud services enable cloudfunctions.googleapis.com
   gcloud services enable cloudscheduler.googleapis.com
   ```

2. **Create and Store Secrets**:
   Store your Meteomatics API credentials in Secret Manager:

   ```sh
   echo -n "your_meteomatics_username" | gcloud secrets create meteomatics_username --data-file=-
   echo -n "your_meteomatics_password" | gcloud secrets create meteomatics_password --data-file=-
   ```

3. **Set Up IAM Roles**:
   Ensure the service account used by Cloud Run and Cloud Functions has the necessary roles:

   ```sh
   gcloud projects add-iam-policy-binding YOUR_PROJECT_ID \
       --member="serviceAccount:YOUR_PROJECT_ID@appspot.gserviceaccount.com" \
       --role="roles/bigquery.dataEditor"

   gcloud projects add-iam-policy-binding YOUR_PROJECT_ID \
       --member="serviceAccount:YOUR_PROJECT_ID@appspot.gserviceaccount.com" \
       --role="roles/secretmanager.secretAccessor"
   ```

### data_fetch

This component is responsible for fetching weather data from Meteomatics and loading it into Google BigQuery.

#### Deploying the Cloud Function

1. **Navigate to the Directory**:

   ```sh
   cd data_fetch
   ```

2. **Deploy the Cloud Function**:

   ```sh
   gcloud functions deploy refresh_weather_data \
       --runtime python39 \
       --trigger-http \
       --allow-unauthenticated
   ```

#### Set Up Cloud Scheduler

1. **Create a Cloud Scheduler Job**:

   ```sh
   gcloud scheduler jobs create http refresh-weather-data-job \
       --schedule "0 0 * * *" \
       --uri "YOUR_CLOUD_FUNCTION_URL" \
       --http-method GET \
       --time-zone "UTC"
   ```

2. **Verify the Cloud Scheduler Job**:

   ```sh
   gcloud scheduler jobs list
   ```

3. **Test the Cloud Scheduler Job**:

   ```sh
   gcloud scheduler jobs run refresh-weather-data-job
   ```

### flask_api

This component provides a Flask API to query weather data stored in Google BigQuery.

#### Running Locally

1. **Clone the Repository**:

   ```sh
   git clone https://github.com/your-repository/flask_api.git
   cd flask_api
   ```

2. **Create a Virtual Environment**:

   ```sh
   python3 -m venv venv
   source venv/bin/activate
   ```

3. **Install Dependencies**:

   ```sh
   pip install -r requirements.txt
   ```

4. **Run the Flask Application**:

   ```sh
   export GOOGLE_CLOUD_PROJECT=your_project_id
   flask run
   ```

#### Deploying to Cloud Run

1. **Build the Docker Image**:

   ```sh
   gcloud builds submit --tag gcr.io/YOUR_PROJECT_ID/flask-api .
   ```

2. **Deploy to Cloud Run**:

   ```sh
   gcloud run deploy flask-api --image gcr.io/YOUR_PROJECT_ID/flask-api --platform managed --region YOUR_REGION --allow-unauthenticated
   ```

3. **Get the Service URL**:

   ```sh
   SERVICE_URL=$(gcloud run services describe flask-api --platform managed --region YOUR_REGION --format 'value(status.url)')
   echo $SERVICE_URL
   ```

4. **Test the Endpoints**:

   ```sh
   curl -X GET $SERVICE_URL/locations
   curl -X GET $SERVICE_URL/latest_forecast
   curl -X GET $SERVICE_URL/average_temperature
   curl -X GET "$SERVICE_URL/top_locations?metric=temperature&n=5"
   ```

### Summary

By following these steps, you can set up the weather data fetching mechanism and the API to query the data. This ensures that your weather data is always up-to-date and accessible via a scalable API.

### Additional Resources

- [Google Cloud SDK Documentation](https://cloud.google.com/sdk/docs)
- [Google BigQuery Documentation](https://cloud.google.com/bigquery/docs)
- [Flask Documentation](https://flask.palletsprojects.com/)
