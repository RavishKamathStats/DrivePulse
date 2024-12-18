import subprocess
import sys

# Fixed Device ID
GPS_DEVICE_ID = '7a09ad70-af4a-11ef-95cd-5dbcd51774f2'
SENSOR_DEVICE_ID = '724240c0-af4a-11ef-95cd-5dbcd51774f2'
def run_script(script_name, *args):
    """
    Run a Python script with optional arguments.
    """
    try:
        command = [sys.executable, script_name] + list(args)
        result = subprocess.run(command, check=True)
        print(f"{script_name} executed successfully.")
    except subprocess.CalledProcessError as e:
        print(f"Error executing {script_name}: {e}")
        sys.exit(1)

if __name__ == "__main__":
    # Ensure trip_id is provided as a command-line argument
    if len(sys.argv) < 2:
        print("Error: Please provide the trip_id as a command-line argument.")
        sys.exit(1)

    trip_id = sys.argv[1]

    print(f"Running handlers for Trip ID: {trip_id}")

    # Run user_info_handler.py
    print("Running user_info_handler.py...")
    run_script("sql/user_info_handler.py", GPS_DEVICE_ID, SENSOR_DEVICE_ID)

    # Run trip_mapping_handler.py
    print("Running trip_mapping_handler.py...")
    run_script("sql/trip_mapping_handler.py", trip_id, GPS_DEVICE_ID, SENSOR_DEVICE_ID)

    # Run telemetry_saver.py
    print("Running telemetry_saver.py...")
    run_script("sql/telemetry_saver.py", trip_id, GPS_DEVICE_ID, SENSOR_DEVICE_ID)

    print("All database updates completed successfully.")