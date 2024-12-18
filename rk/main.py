import os
import uuid
import time


def run_in_background(command):
    """
    Run a command in the background using &.
    """
    os.system(f"{command} &")


if __name__ == "__main__":
    # Generate a unique trip_id
    trip_id = str(uuid.uuid4())
    print(f"Generated Trip ID: {trip_id}")

    # Remove the stop signal file if it exists
    stop_signal_file = os.path.join(os.getcwd(), "stop_telemetry")
    if os.path.exists(stop_signal_file):
        print("Removing old stop signal file...")
        os.remove(stop_signal_file)

    # Check script dependencies
    required_scripts = [
        "data_collection/gps.py",
        "data_collection/sensor.py",
        "sql/run_all_handlers.py"
    ]
    for script in required_scripts:
        if not os.path.exists(script):
            print(f"Error: {script} not found.")
            exit(1)

    # Run gps.py and sensor.py in the background
    print("Starting gps.py and sensor.py...")
    run_in_background(f"python data_collection/gps.py {trip_id}")
    run_in_background(f"python data_collection/sensor.py {trip_id}")

    try:
        # Wait for user input to stop telemetry collection
        input("Press Enter to stop telemetry collection...\n")

        # Create the stop signal file to signal both scripts to terminate
        with open(stop_signal_file, "w") as f:
            f.write("stop")

        # Wait briefly to ensure the scripts detect the stop signal
        time.sleep(2)

    except KeyboardInterrupt:
        print("\nUser interrupted. Terminating telemetry collection...")

    print("Telemetry collection stopped.")

    # Run the SQL handler orchestrator script
    print("Running sql/run_all_handlers.py to update the database...")
    try:
        os.system(f"python sql/run_all_handlers.py {trip_id}")
        print("Database update completed successfully.")
    except Exception as e:
        print(f"Error running sql/run_all_handlers.py: {e}")
        exit(1)

    # Run the scoring orchestrator script
    print("Running scoring/run_all_scoring.py to calculate scores...")
    try:
        os.system(f"python scoring/run_all_scoring.py {trip_id}")
        print("Scoring completed successfully.")
    except Exception as e:
        print(f"Error running scoring/run_all_scoring.py: {e}")
        exit(1)
