import logging
from flask import Flask, request, jsonify
from google.cloud import bigquery
import os

# Initialize Flask app
app = Flask(__name__)

# Initialize BigQuery client
client = bigquery.Client()

# Set up logging
logging.basicConfig(level=logging.INFO)
project_id='margeraweatherapp'

# Endpoint to list all distinct locations
@app.route('/locations', methods=['GET'])
def list_locations():
    try:
        query = f"SELECT DISTINCT location FROM `{project_id}.weather_data.forecasts`"
        logging.info(f"Executing query: {query}")
        results = client.query(query).result()
        locations = [row['location'] for row in results]
        logging.info(f"Locations retrieved: {locations}")
        return jsonify(locations)
    except Exception as e:
        logging.error(f"Error in /locations endpoint: {e}")
        logging.error(f"Exception: {e}", exc_info=True)
        return "Internal Server Error", 500

# Endpoint to get the latest forecast date for each location
@app.route('/latest_forecast', methods=['GET'])
def latest_forecast():
    try:
        query = f"""
        WITH
            latest_dates AS (
            SELECT
                location,
                MAX(date) AS latest_date
            FROM
                `margeraweatherapp.weather_data.forecasts`
            GROUP BY
                location )
            SELECT
            f.location,
            f.date AS latest_date,
            f.temperature,
            f.precipitation,
            f.wind_speed,
            f.wind_direction
            FROM
            `margeraweatherapp.weather_data.forecasts` f
            JOIN
            latest_dates ld
            ON
            f.location = ld.location
            AND f.date = ld.latest_date
        """
        logging.info(f"Executing query: {query}")
        results = client.query(query).result()
        forecasts = [
                        {
                            'location': row['location'],
                            'latest_date': row['latest_date'],
                            'temperature': row['temperature'],
                            'precipitation': row['precipitation'],
                            'wind_speed': row['wind_speed'],
                            'wind_direction': row['wind_direction']
                        } for row in results
                    ]
        logging.info(f"Latest forecasts retrieved: {forecasts}")
        return jsonify(forecasts)
    except Exception as e:
        logging.error(f"Error in /latest_forecast endpoint: {e}")
        logging.error(f"Exception: {e}", exc_info=True)
        return "Internal Server Error", 500

# Endpoint to get the average temperature of the last 3 forecasts for each location
@app.route('/average_temperature', methods=['GET'])
def average_temperature():
    try:
        query = f"""
        SELECT location, AVG(temperature) as avg_temp
        FROM (
            SELECT location, temperature
            FROM `{project_id}.weather_data.forecasts`
            ORDER BY date DESC
            LIMIT 3
        )
        GROUP BY location
        """
        logging.info(f"Executing query: {query}")
        results = client.query(query).result()
        avg_temps = [{'location': row['location'], 'avg_temp': row['avg_temp']} for row in results]
        logging.info(f"Average temperatures retrieved: {avg_temps}")
        return jsonify(avg_temps)
    except Exception as e:
        logging.error(f"Error in /average_temperature endpoint: {e}")
        logging.error(f"Exception: {e}", exc_info=True)
        return "Internal Server Error", 500

# Endpoint to get the top n locations based on a specified metric
@app.route('/top_locations', methods=['GET'])
def top_locations():
    try:
        metric = request.args.get('metric')
        n = int(request.args.get('n', 5))
        query = f"""
        SELECT location, AVG({metric}) as avg_{metric}
        FROM `{project_id}.weather_data.forecasts`
        GROUP BY location
        ORDER BY avg_metric DESC
        LIMIT {n}
        """
        logging.info(f"Executing query: {query}")
        results = client.query(query).result()
        top_locations = [{'location': row['location'], f'avg_{metric}': row[f'avg_{metric}']} for row in results]
        logging.info(f"Top locations retrieved: {top_locations}")
        return jsonify(top_locations)
    except Exception as e:
        logging.error(f"Error in /top_locations endpoint: {e}")
        logging.error(f"Exception: {e}", exc_info=True)
        return "Internal Server Error", 500

# Run the Flask app
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port)
