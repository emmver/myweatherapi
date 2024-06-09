import os
import requests
import datetime
from google.cloud import bigquery
from google.cloud import secretmanager
import logging

logging.basicConfig(level=logging.INFO)

API_URL = "https://api.meteomatics.com"

def check_environment_variables():
    logging.info("Checking environment variables...")
    for key, value in os.environ.items():
        logging.info(f"{key}: {value}")

def get_secret(secret_name):
    client = secretmanager.SecretManagerServiceClient()
    project_number = os.environ.get('GOOGLE_CLOUD_PROJECT')
    if not project_number:
        logging.error("GOOGLE_CLOUD_PROJECT environment variable not set.")
        raise Exception("GOOGLE_CLOUD_PROJECT environment variable not set.")
    secret_path = f"projects/{project_number}/secrets/{secret_name}/versions/latest"
    response = client.access_secret_version(request={"name": secret_path})
    return response.payload.data.decode("UTF-8")

def fetch_weather_data(location):
    start_date = datetime.datetime.now()
    end_date = start_date + datetime.timedelta(days=7)
    timeframe = f"{start_date.strftime('%Y-%m-%d')}T00:00:00Z--{end_date.strftime('%Y-%m-%d')}T00:00:00Z:PT24H"
    parameters = 't_2m:C,precip_24h:mm,wind_speed_10m:ms,wind_dir_10m:d'
    response = requests.get(f"{API_URL}/{timeframe}/{parameters}/{location}/json?model=mix", auth=(USERNAME, PASSWORD))
    response.raise_for_status()
    return response.json()

def transform_data(location, data):
    transformed_data = []
    timestamps = [point['date'] for point in data['data'][0]['coordinates'][0]['dates']]
    
    for i, timestamp in enumerate(timestamps):
        entry = {
            'location': location,
            'date': timestamp,
            'temperature': data['data'][0]['coordinates'][0]['dates'][i]['value'],
            'precipitation': data['data'][1]['coordinates'][0]['dates'][i]['value'],
            'wind_speed': data['data'][2]['coordinates'][0]['dates'][i]['value'],
            'wind_direction': data['data'][3]['coordinates'][0]['dates'][i]['value'],
        }
        transformed_data.append(entry)
    
    return transformed_data

def load_data_to_bq(data, dataset_id, table_id):
    client = bigquery.Client()
    table_ref = client.dataset(dataset_id).table(table_id)
    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("location", "STRING"),
            bigquery.SchemaField("date", "TIMESTAMP"),
            bigquery.SchemaField("temperature", "FLOAT"),
            bigquery.SchemaField("precipitation", "FLOAT"),
            bigquery.SchemaField("wind_speed", "FLOAT"),
            bigquery.SchemaField("wind_direction", "FLOAT"),
        ],
        write_disposition="WRITE_TRUNCATE",
    )
    job = client.load_table_from_json(data, table_ref, job_config=job_config)
    job.result()
    logging.info(f"Loaded {job.output_rows} rows into {dataset_id}:{table_id}.")

def main():
    global USERNAME, PASSWORD
    check_environment_variables()
    USERNAME = get_secret("metomatics_username")
    PASSWORD = get_secret("meteomatics_password")

    locations = {
        "athens": "37.9838,23.7275",
        "limassol": "34.6786,33.0413",
        "berlin": "52.5200,13.4050"
    }

    all_transformed_data = []
    for loc_name, loc_coords in locations.items():
        raw_data = fetch_weather_data(loc_coords)
        transformed_data = transform_data(loc_name, raw_data)
        all_transformed_data.extend(transformed_data)

    dataset_id = 'weather_data'
    table_id = 'forecasts'
    load_data_to_bq(all_transformed_data, dataset_id, table_id)

    return 'Weather data refreshed successfully!'

def refresh_weather_data(request):
    try:
        result = main()
        return result, 200
    except Exception as e:
        logging.error(f"Error in refresh_weather_data: {e}")
        return str(e), 500