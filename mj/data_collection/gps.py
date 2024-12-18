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
gps_data = []

# Configuration
stop_signal_file = os.path.join(os.getcwd(), "stop_telemetry")
url = "http://54.241.86.221:8080/api/v1/2JBxFuT4r3CgUQjd3u0g/telemetry"
headers = {"Content-Type": "application/json"}

last_timestamp = time.time()

while not os.path.exists(stop_signal_file):
    try:
        current_time = time.time()
        if current_time - last_timestamp >= 1:
            last_timestamp = current_time

            # Get timestamps
            readable_ts = datetime.now().isoformat()

            # Fetch GPS data from termux-location
            gpscmd = scrip.compute("termux-location")
            gps = json.loads(gpscmd["output"])

            # Append the data to the list
            gps_data.append({
                "trip_id": trip_id,
                "timestamp": readable_ts,
                "latitude": gps.get("latitude"),
                "longitude": gps.get("longitude"),
                "bearing": gps.get("bearing"),
                "speed": gps.get("speed")
            })

            # print(f"GPS data collected")

    except Exception as e:
        print(f"An error occurred: {e}")

print("GPS script terminated. Sending data to ThingsboardIO...")

# Send the list of dictionaries as JSON to ThingsboardIO
try:
    for data in gps_data:
        response = requests.post(url, headers=headers, json=data)
    if response.status_code == 200:
        print("GPS data sent successfully.")
    else:
        print(f"Failed to send GPS data: {response.status_code}, {response.text}")
except Exception as e:
    print(f"Failed to send GPS data: {e}")