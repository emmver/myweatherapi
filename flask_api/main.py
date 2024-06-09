from flask import Flask, request, jsonify
from google.cloud import bigquery
import os

# Initialize Flask app
app = Flask(__name__)

# Initialize BigQuery client
client = bigquery.Client()

# Endpoint to list all distinct locations
@app.route('/locations', methods=['GET'])
def list_locations():
    # BigQuery query to get distinct locations
    query = "SELECT DISTINCT location FROM `your_project_id.weather_data.forecasts`"
    # Execute query and get results
    results = client.query(query).result()
    # Extract locations from query results
    locations = [row['location'] for row in results]
    # Return locations as JSON response
    return jsonify(locations)

# Endpoint to get the latest forecast date for each location
@app.route('/latest_forecast', methods=['GET'])
def latest_forecast():
    # BigQuery query to get the latest forecast date for each location
    query = """
    SELECT location, MAX(date) as latest_date
    FROM `your_project_id.weather_data.forecasts`
    GROUP BY location
    """
    # Execute query and get results
    results = client.query(query).result()
    # Extract location and latest forecast date from query results
    forecasts = [{'location': row['location'], 'latest_date': row['latest_date']} for row in results]
    # Return forecasts as JSON response
    return jsonify(forecasts)

# Endpoint to get the average temperature of the last 3 forecasts for each location
@app.route('/average_temperature', methods=['GET'])
def average_temperature():
    # BigQuery query to get the average temperature of the last 3 forecasts for each location
    query = """
    SELECT location, AVG(temperature) as avg_temp
    FROM (
        SELECT location, temperature
        FROM `your_project_id.weather_data.forecasts`
        ORDER BY date DESC
        LIMIT 3
    )
    GROUP BY location
    """
    # Execute query and get results
    results = client.query(query).result()
    # Extract location and average temperature from query results
    avg_temps = [{'location': row['location'], 'avg_temp': row['avg_temp']} for row in results]
    # Return average temperatures as JSON response
    return jsonify(avg_temps)

# Endpoint to get the top n locations based on a specified metric
@app.route('/top_locations', methods=['GET'])
def top_locations():
    # Get metric and n parameters from the request
    metric = request.args.get('metric')
    n = int(request.args.get('n', 5))
    # BigQuery query to get the top n locations based on the specified metric
    query = f"""
    SELECT location, AVG({metric}) as avg_metric
    FROM `your_project_id.weather_data.forecasts`
    GROUP BY location
    ORDER BY avg_metric DESC
    LIMIT {n}
    """
    # Execute query and get results
    results = client.query(query).result()
    # Extract location and average metric from query results
    top_locations = [{'location': row['location'], f'avg_{metric}': row[f'avg_{metric}']} for row in results]
    # Return top locations as JSON response
    return jsonify(top_locations)

# Run the Flask app
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port)
