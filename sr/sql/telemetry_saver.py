import sys
import os
sys.path.append(
    os.path.abspath(
        os.path.join(os.path.dirname(__file__), "../..")
    )
)
from sql.database import connect_to_mysql
from mysql.connector import Error
import datetime
import requests
import json

# Configuration for ThingsBoard IO
THINGSBOARD_URL = 'http://54.241.86.221:8080'

def save_driving_data_to_mysql(trip_id, driving_data):
    """
    Save driving data to the driving_data table.
    """
    connection = connect_to_mysql()
    if not connection:
        return

    try:
        cursor = connection.cursor()

        # Create the driving_data table if it doesn't exist
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS driving_data (
                trip_id VARCHAR(255),
                timestamp VARCHAR(255),
                latitude DOUBLE,
                longitude DOUBLE,
                bearing DOUBLE,
                speed DOUBLE,
                PRIMARY KEY (trip_id, timestamp),
                FOREIGN KEY (trip_id)
                    REFERENCES device_trip_mapping(trip_id)
                    ON DELETE CASCADE
            )
        ''')

        for entry in driving_data:
            cursor.execute('''
                INSERT INTO driving_data (
                    trip_id, timestamp, latitude, longitude, bearing, speed
                ) VALUES (%s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    latitude = VALUES(latitude),
                    longitude = VALUES(longitude),
                    bearing = VALUES(bearing),
                    speed = VALUES(speed)
            ''', (
                entry["trip_id"],
                entry["timestamp"],
                entry.get("latitude"),
                entry.get("longitude"),
                entry.get("bearing"),
                entry.get("speed"),
            ))

        connection.commit()
        print("GPS data saved successfully.")
    except Error as e:
        print(f"Error saving GPS data: {e}")
    finally:
        connection.close()

def save_sensors_data_to_mysql(trip_id, sensor_data):
    """
    Save sensor data to the sensors_data table.
    """
    connection = connect_to_mysql()
    if not connection:
        return

    try:
        cursor = connection.cursor()

        # Create the sensors_data table if it doesn't exist
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS sensors_data (
                trip_id VARCHAR(255),
                timestamp VARCHAR(255),
                accelerometer JSON,
                gyroscope JSON,
                linear_acceleration JSON,
                magnetometer JSON,
                PRIMARY KEY (trip_id, timestamp),
                FOREIGN KEY (trip_id)
                    REFERENCES device_trip_mapping(trip_id)
                    ON DELETE CASCADE
            )
        ''')

        for entry in sensor_data:
            cursor.execute('''
                INSERT INTO sensors_data (
                    trip_id, timestamp, accelerometer, gyroscope, linear_acceleration, magnetometer
                ) VALUES (%s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    accelerometer = VALUES(accelerometer),
                    gyroscope = VALUES(gyroscope),
                    linear_acceleration = VALUES(linear_acceleration),
                    magnetometer = VALUES(magnetometer)
            ''', (
                entry["trip_id"],
                entry["timestamp"],
                entry["accelerometer"] if isinstance(entry["accelerometer"], str) else json.dumps(entry["accelerometer"]),
                entry["gyroscope"] if isinstance(entry["gyroscope"], str) else json.dumps(entry["gyroscope"]),
                entry["linear_acceleration"] if isinstance(entry["linear_acceleration"], str) else json.dumps(entry["linear_acceleration"]),
                entry["magnetometer"] if isinstance(entry["magnetometer"], str) else json.dumps(entry["magnetometer"]),
            ))

        connection.commit()
        print("Sensor data saved successfully.")
    except Error as e:
        print(f"Error saving sensor data: {e}")
    finally:
        connection.close()

# Functions for ThingsBoard API
def fetch_thingsboard_data(jwt_token, device_id, keys, start_ts, end_ts):
    """
    Fetch telemetry data from ThingsBoard for the specified device ID, keys, and time range.
    """
    try:
        url = f"{THINGSBOARD_URL}/api/plugins/telemetry/DEVICE/{device_id}/values/timeseries"
        params = {
            "keys": ",".join(keys),
            "startTs": start_ts,
            "endTs": end_ts,
            "agg": "NONE",
            "limit": 10000000,
        }
        headers = {"X-Authorization": f"Bearer {jwt_token}"}
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f"Error fetching data from ThingsBoard for device {device_id}: {e}")
        return {}

def get_jwt_token():
    """
    Authenticate with ThingsBoard and retrieve a JWT token.
    """
    try:
        response = requests.post(
            f"{THINGSBOARD_URL}/api/auth/login",
            headers={"Content-Type": "application/json"},
            json={"username": "tenant@thingsboard.org", "password": "tenant"}
        )
        response.raise_for_status()
        return response.json()["token"]
    except requests.RequestException as e:
        print(f"Error during authentication: {e}")
        sys.exit(1)

if __name__ == "__main__":
    # Accept trip_id, gps_device_id, and sensor_device_id as command-line arguments
    if len(sys.argv) > 3:
        trip_id = sys.argv[1]
        gps_device_id = sys.argv[2]
        sensor_device_id = sys.argv[3]
    else:
        print("Error: Please provide the trip_id, gps_device_id, and sensor_device_id as command-line arguments.")
        sys.exit(1)

    # Authenticate with ThingsBoard to get the JWT token
    jwt_token = get_jwt_token()

    # Define the time range for fetching data
    start_time = 0  # Replace with the desired start timestamp in milliseconds
    end_time = int(datetime.datetime.now().timestamp() * 1000)  # Current time in milliseconds

    try:
        # Fetch GPS data from ThingsBoard
        print("Fetching GPS data...")
        gps_data = fetch_thingsboard_data(
            jwt_token, 
            gps_device_id,
            ["latitude", "longitude", "bearing", "speed", "timestamp", "trip_id"],
            start_time,
            end_time
        )

        # Debugging: Print raw GPS data
        # print("GPS Data:", json.dumps(gps_data, indent=4))  # Pretty print GPS data for inspection

        # Process GPS data entries
        gps_entries = []
        if "timestamp" in gps_data and "trip_id" in gps_data:
            gps_entries = [
                {
                    "trip_id": gps_data.get("trip_id", [{}])[i].get("value"),
                    "timestamp": str(gps_data.get("timestamp", [{}])[i].get("value")),  # Ensure ISO format is preserved
                    "latitude": gps_data.get("latitude", [{}])[i].get("value"),
                    "longitude": gps_data.get("longitude", [{}])[i].get("value"),
                    "bearing": gps_data.get("bearing", [{}])[i].get("value"),
                    "speed": gps_data.get("speed", [{}])[i].get("value"),
                }
                for i in range(len(gps_data["timestamp"]))
                if gps_data.get("trip_id", [{}])[i].get("value") == trip_id  # Filter by trip_id
            ]

        # Save filtered GPS data to the database
        if gps_entries:
            print(f"Saving {len(gps_entries)} GPS entries to the database...")
            save_driving_data_to_mysql(trip_id, gps_entries)
        else:
            print("No matching GPS data available to save.")

        # Fetch sensor data from ThingsBoard
        print("Fetching sensor data...")
        sensor_data = fetch_thingsboard_data(
            jwt_token, 
            sensor_device_id, 
            ["accelerometer", "gyroscope", "linear_acceleration", "magnetometer", "timestamp", "trip_id"],
            start_time,
            end_time
        )

        # Debugging: Print raw sensor data
        # print("Sensor Data:", json.dumps(sensor_data, indent=4))  # Pretty print sensor data for inspection

        # Process sensor data entries
        sensor_entries = []
        if "timestamp" in sensor_data and "trip_id" in sensor_data:
            sensor_entries = [
                {
                    "trip_id": sensor_data.get("trip_id", [{}])[i].get("value"),
                    "timestamp": str(sensor_data.get("timestamp", [{}])[i].get("value")),  # Ensure ISO format is preserved
                    "accelerometer": sensor_data.get("accelerometer", [{}])[i].get("value"),  # Direct value
                    "gyroscope": sensor_data.get("gyroscope", [{}])[i].get("value"),        # Direct value
                    "linear_acceleration": sensor_data.get("linear_acceleration", [{}])[i].get("value"),  # Direct value
                    "magnetometer": sensor_data.get("magnetometer", [{}])[i].get("value"),  # Direct value
                }
                for i in range(len(sensor_data["timestamp"]))
                if sensor_data.get("trip_id", [{}])[i].get("value") == trip_id  # Filter by trip_id
            ]

        # Save filtered sensor data to the database
        if sensor_entries:
            print(f"Saving {len(sensor_entries)} sensor entries to the database...")
            save_sensors_data_to_mysql(trip_id, sensor_entries)
        else:
            print("No matching sensor data available to save.")

        print("Telemetry data saving process completed.")

    except Exception as e:
        print(f"An error occurred: {e}")