import requests
import json
import time
from datetime import datetime
from pimux import scrip
import os
import sys

# Get trip_id from command-line arguments
trip_id = sys.argv[1] if len(sys.argv) > 1 else "default_trip_id"

# Initialize a list to collect data
sensor_data = []

# Configuration
stop_signal_file = os.path.join(os.getcwd(), "stop_telemetry")
url = "http://54.241.86.221:8080/api/v1/vOMc1ws0svV3ePVTwxt3/telemetry"
headers = {"Content-Type": "application/json"}

# Mapping of desired keys to actual sensor names
sensors = {
    "linear_acceleration": "linear_acceleration",
    "accelerometer": "icm4x6xa Accelerometer Wakeup",
    "magnetometer": "qmc6308 Magnetometer Wakeup",
    "gyroscope": "icm4x6xa Gyroscope Wakeup"
}

# Combine all sensor names into a single query
sensor_query = ','.join(sensors.values())

while not os.path.exists(stop_signal_file):
    start_time = time.time()  # Capture the start time for the loop

    try:
        # Get timestamps
        readable_ts = datetime.now().isoformat()
        row_data = {"trip_id": trip_id, "timestamp": readable_ts}

        # Fetch data for all sensors in one call
        try:
            sensor_cmd = scrip.compute(f"termux-sensor -s '{sensor_query}' -n 1")
            sensor_outputs = json.loads(sensor_cmd["output"])

            for desired_key, sensor_name in sensors.items():
                # Extract values for each sensor using its actual name
                sensor_output = sensor_outputs.get(sensor_name, {})
                sensor_values = sensor_output.get("values", [None, None, None])

                # Map the values to x, y, z for the desired key
                row_data[desired_key] = {
                    "x": sensor_values[0],
                    "y": sensor_values[1],
                    "z": sensor_values[2]
                }

        except Exception as sensor_error:
            print(f"Error collecting sensor data: {sensor_error}")
            for desired_key in sensors.keys():
                row_data[desired_key] = {"x": None, "y": None, "z": None}

        # Append the row to the list
        sensor_data.append(row_data)

    except Exception as e:
        print(f"An error occurred: {e}")

    # Ensure a fixed 1-second interval
    elapsed_time = time.time() - start_time
    if elapsed_time < 1:
        time.sleep(1 - elapsed_time)

print("Sensor script terminated. Sending data to ThingsboardIO...")

# Send the list of dictionaries as JSON to ThingsboardIO
try:
    for data in sensor_data:
        response = requests.post(url, headers=headers, json=data)
    if response.status_code == 200:
        print("Sensor data sent successfully.")
    else:
        print(f"Failed to send sensor data: {response.status_code}, {response.text}")
except Exception as e:
    print(f"Failed to send sensor data: {e}")