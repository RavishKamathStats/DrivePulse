import sys
import os

# Add the parent directory of `testing_user` to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

import math
from mysql.connector import Error
from datetime import datetime, timedelta
from mj.sql.database import connect_to_mysql
from mj.sql.database import MYSQL_DATABASE

def preprocess_timestamp_data(trip_id):
    """
    Preprocess driving data for the given trip_id by filling in
    missing timestamps and saving the preprocessed data to a new table.
    """
    connection = None
    try:
        # Step 1: Connect to MySQL
        connection = connect_to_mysql()
        cursor = connection.cursor()

        # Step 2: Fetch the start and end timestamps for the trip_id
        cursor.execute("""
            SELECT MIN(timestamp), MAX(timestamp)
            FROM driving_data
            WHERE trip_id = %s
        """, (trip_id,))
        start_time_str, end_time_str = cursor.fetchone()

        # Ensure the trip_id exists in the database
        if not start_time_str or not end_time_str:
            print(f"No data found for trip_id: {trip_id}")
            return

        # Convert timestamps from ISO 8601 format
        # (with 'T') to datetime objects
        start_time = datetime.strptime(start_time_str, "%Y-%m-%dT%H:%M:%S.%f")
        end_time = datetime.strptime(end_time_str, "%Y-%m-%dT%H:%M:%S.%f")

        # Step 3: Generate a list of all timestamps at 1-second intervals
        current_time = start_time
        timestamps = []
        while current_time <= end_time:
            timestamps.append(current_time)
            current_time += timedelta(seconds=1)

        # Step 4: Fetch existing timestamps for the trip_id
        cursor.execute("""
            SELECT timestamp
            FROM driving_data
            WHERE trip_id = %s
        """, (trip_id,))
        existing_timestamps = {
            datetime.strptime(row[0], "%Y-%m-%dT%H:%M:%S.%f") for row in cursor.fetchall()
        }

        # Step 5: Find missing timestamps
        missing_timestamps = set(timestamps) - existing_timestamps

        # Step 6: Create a new SQL table for preprocessed data
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS preprocessed_driving_data (
                trip_id VARCHAR(255),
                timestamp VARCHAR(255),
                latitude DOUBLE NULL,
                longitude DOUBLE NULL,
                bearing DOUBLE NULL,
                speed DOUBLE NULL,
                PRIMARY KEY (trip_id, timestamp)
            )
        """)
        connection.commit()

        # Step 7: Copy existing data to the new table
        cursor.execute("""
            INSERT IGNORE INTO preprocessed_driving_data (trip_id, timestamp, latitude, longitude, bearing, speed)
            SELECT trip_id, timestamp, latitude, longitude, bearing, speed
            FROM driving_data
            WHERE trip_id = %s
        """, (trip_id,))
        connection.commit()

        # Step 8: Insert missing timestamps with NULL values into the new table
        for ts in missing_timestamps:
            # Convert the datetime object back to ISO 8601 format string
            ts_str = ts.strftime("%Y-%m-%dT%H:%M:%S.%f")
            cursor.execute("""
                INSERT IGNORE INTO preprocessed_driving_data (trip_id, timestamp, latitude, longitude, bearing, speed)
                VALUES (%s, %s, NULL, NULL, NULL, NULL)
            """, (trip_id, ts_str))

        # Commit the changes
        connection.commit()
    except Error as e:
        print(f"Error: {e}")
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()

def interpolate_lat_lon(trip_id):
    """
    Interpolate missing latitude and longitude values in the
    preprocessed_driving_data table for the given trip_id.
    """
    connection = None
    try:
        # Step 1: Connect to MySQL
        connection = connect_to_mysql()
        cursor = connection.cursor(dictionary=True)

        # Step 2: Fetch all data for the trip_id, ordered by timestamp
        cursor.execute("""
            SELECT timestamp, latitude, longitude
            FROM preprocessed_driving_data
            WHERE trip_id = %s
            ORDER BY timestamp
        """, (trip_id,))
        data = cursor.fetchall()

        if not data:
            print(f"No data found for trip_id: {trip_id}.")
            return

        # Parse data into separate lists
        timestamps = [row['timestamp'] for row in data]
        latitudes = [row['latitude'] for row in data]
        longitudes = [row['longitude'] for row in data]

        # Step 3: Linear interpolation for latitude and longitude
        def interpolate(values):
            for i in range(len(values)):
                if values[i] is None:
                    # Find nearest valid values before and after
                    prev_idx = next((j for j in range(i - 1, -1, -1) if values[j] is not None), None)
                    next_idx = next((j for j in range(i + 1, len(values)) if values[j] is not None), None)

                    if prev_idx is not None and next_idx is not None:
                        # Linear interpolation
                        time_before = datetime.strptime(timestamps[prev_idx], "%Y-%m-%dT%H:%M:%S.%f")
                        time_after = datetime.strptime(timestamps[next_idx], "%Y-%m-%dT%H:%M:%S.%f")
                        value_before = values[prev_idx]
                        value_after = values[next_idx]

                        time_current = datetime.strptime(timestamps[i], "%Y-%m-%dT%H:%M:%S.%f")
                        time_diff = (time_after - time_before).total_seconds()
                        time_to_current = (time_current - time_before).total_seconds()

                        values[i] = value_before + (value_after - value_before) * (time_to_current / time_diff)
                    elif prev_idx is not None:
                        # Forward fill
                        values[i] = values[prev_idx]
                    elif next_idx is not None:
                        # Backward fill
                        values[i] = values[next_idx]
            return values

        # Interpolate latitude and longitude
        latitudes = interpolate(latitudes)
        longitudes = interpolate(longitudes)

        # Step 4: Update the database with interpolated values
        for i, ts in enumerate(timestamps):
            cursor.execute("""
                UPDATE preprocessed_driving_data
                SET latitude = %s, longitude = %s
                WHERE trip_id = %s AND timestamp = %s
            """, (latitudes[i], longitudes[i], trip_id, ts))

        connection.commit()

    except Error as e:
        print(f"Error: {e}")
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()

def calculate_bearing(trip_id):
    """
    Calculate the bearing values using latitude and longitude for a given trip_id
    and save them in the bearing_gps column in the preprocessed_driving_data table.
    """
    connection = None
    try:
        # Step 1: Connect to MySQL
        connection = connect_to_mysql()
        cursor = connection.cursor(dictionary=True)

        # Step 2: Check if the column `bearing_gps` exists
        cursor.execute("""
            SELECT COUNT(*)
            FROM information_schema.columns
            WHERE table_name = 'preprocessed_driving_data'
            AND table_schema = %s
            AND column_name = 'bearing_gps'
        """, (MYSQL_DATABASE,))
        if cursor.fetchone()['COUNT(*)'] == 0:
            # Add the `bearing_gps` column
            cursor.execute("""
                ALTER TABLE preprocessed_driving_data
                ADD COLUMN bearing_gps DOUBLE NULL
            """)
            connection.commit()

        # Step 3: Fetch all data for the trip_id, ordered by timestamp
        cursor.execute("""
            SELECT timestamp, latitude, longitude
            FROM preprocessed_driving_data
            WHERE trip_id = %s
            ORDER BY timestamp
        """, (trip_id,))
        data = cursor.fetchall()

        if not data:
            print(f"No data found for trip_id: {trip_id}.")
            return

        # Step 4: Calculate bearing for each row
        bearings = [None] * len(data)
        for i in range(1, len(data)):
            lat1 = math.radians(data[i - 1]['latitude'])
            lon1 = math.radians(data[i - 1]['longitude'])
            lat2 = math.radians(data[i]['latitude'])
            lon2 = math.radians(data[i]['longitude'])

            delta_lon = lon2 - lon1

            x = math.sin(delta_lon) * math.cos(lat2)
            y = math.cos(lat1) * math.sin(lat2) - math.sin(lat1) * math.cos(lat2) * math.cos(delta_lon)
            initial_bearing = math.atan2(x, y)

            # Convert bearing from radians to degrees and normalize to [0, 360]
            bearing = (math.degrees(initial_bearing) + 360) % 360
            bearings[i] = bearing

        # Step 5: Update the database with calculated bearing values
        for i, row in enumerate(data):
            cursor.execute("""
                UPDATE preprocessed_driving_data
                SET bearing_gps = %s
                WHERE trip_id = %s AND timestamp = %s
            """, (bearings[i], trip_id, row['timestamp']))

        connection.commit()

    except Error as e:
        print(f"Error: {e}")
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()

def calculate_moving_average(trip_id):
    """
    Calculate a centered moving average for the bearing_gps column
    with a 3-second window and save it in the bearing_gps_avg column.
    """
    connection = None
    try:
        # Step 1: Connect to MySQL
        connection = connect_to_mysql()
        cursor = connection.cursor(dictionary=True)

        # Step 2: Add the bearing_gps_avg column if it does not exist
        cursor.execute("""
            SELECT COUNT(*)
            FROM information_schema.columns
            WHERE table_name = 'preprocessed_driving_data'
            AND table_schema = %s
            AND column_name = 'bearing_gps_avg'
        """, (MYSQL_DATABASE,))
        if cursor.fetchone()['COUNT(*)'] == 0:
            # Add the bearing_gps_avg column
            cursor.execute("""
                ALTER TABLE preprocessed_driving_data
                ADD COLUMN bearing_gps_avg DOUBLE NULL
            """)
            connection.commit()

        # Step 3: Fetch all data for the trip_id, ordered by timestamp
        cursor.execute("""
            SELECT timestamp, bearing_gps
            FROM preprocessed_driving_data
            WHERE trip_id = %s
            ORDER BY timestamp
        """, (trip_id,))
        data = cursor.fetchall()

        if not data:
            print(f"No data found for trip_id: {trip_id}.")
            return

        # Step 4: Calculate the centered moving average
        moving_averages = [None] * len(data)
        for i in range(len(data)):
            # Use the previous, current, and next rows
            values = []
            if i > 0 and data[i - 1]['bearing_gps'] is not None:
                values.append(data[i - 1]['bearing_gps'])
            if data[i]['bearing_gps'] is not None:
                values.append(data[i]['bearing_gps'])
            if i < len(data) - 1 and data[i + 1]['bearing_gps'] is not None:
                values.append(data[i + 1]['bearing_gps'])

            # Calculate the average if there are valid values
            if values:
                moving_averages[i] = sum(values) / len(values)

        # Step 5: Update the database with calculated moving averages
        for i, row in enumerate(data):
            cursor.execute("""
                UPDATE preprocessed_driving_data
                SET bearing_gps_avg = %s
                WHERE trip_id = %s AND timestamp = %s
            """, (moving_averages[i], trip_id, row['timestamp']))

        connection.commit()

    except Error as e:
        print(f"Error: {e}")
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()

def calculate_speed(trip_id):
    """
    Calculate speed in miles per hour (mph) based on latitude, longitude, and timestamp
    for a given trip_id and save it in the speed_gps column.
    """
    connection = None
    try:
        # Step 1: Connect to MySQL
        connection = connect_to_mysql()
        cursor = connection.cursor(dictionary=True)

        # Step 2: Add the speed_gps column if it does not exist
        cursor.execute("""
            SELECT COUNT(*)
            FROM information_schema.columns
            WHERE table_name = 'preprocessed_driving_data'
            AND table_schema = %s
            AND column_name = 'speed_gps'
        """, (MYSQL_DATABASE,))
        if cursor.fetchone()['COUNT(*)'] == 0:
            # Add the speed_gps column
            cursor.execute("""
                ALTER TABLE preprocessed_driving_data
                ADD COLUMN speed_gps DOUBLE NULL
            """)
            connection.commit()

        # Step 3: Fetch all data for the trip_id, ordered by timestamp
        cursor.execute("""
            SELECT timestamp, latitude, longitude
            FROM preprocessed_driving_data
            WHERE trip_id = %s
            ORDER BY timestamp
        """, (trip_id,))
        data = cursor.fetchall()

        if not data:
            print(f"No data found for trip_id: {trip_id}.")
            return

        # Step 4: Calculate speed for each row in mph
        speeds = [None] * len(data)
        R = 6371000  # Earth's radius in meters

        for i in range(1, len(data)):
            lat1 = math.radians(data[i - 1]['latitude'])
            lon1 = math.radians(data[i - 1]['longitude'])
            lat2 = math.radians(data[i]['latitude'])
            lon2 = math.radians(data[i]['longitude'])

            delta_lat = lat2 - lat1
            delta_lon = lon2 - lon1

            # Haversine formula to calculate distance
            a = math.sin(delta_lat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(delta_lon / 2) ** 2
            c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
            distance = R * c  # Distance in meters

            # Calculate time difference in seconds
            time1 = datetime.strptime(data[i - 1]['timestamp'], "%Y-%m-%dT%H:%M:%S.%f")
            time2 = datetime.strptime(data[i]['timestamp'], "%Y-%m-%dT%H:%M:%S.%f")
            time_diff = (time2 - time1).total_seconds()

            # Calculate speed (mph)
            if time_diff > 0:
                speeds[i] = (distance / time_diff) * 2.23694  # Convert m/s to mph

        # Step 5: Update the database with calculated speed values
        for i, row in enumerate(data):
            cursor.execute("""
                UPDATE preprocessed_driving_data
                SET speed_gps = %s
                WHERE trip_id = %s AND timestamp = %s
            """, (speeds[i], trip_id, row['timestamp']))

        connection.commit()

    except Error as e:
        print(f"Error: {e}")
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()

def calculate_speed_moving_average(trip_id):
    """
    Calculate a centered moving average for the speed_gps column
    with a 3-second window and save it in the speed_gps_avg column.
    """
    connection = None
    try:
        # Step 1: Connect to MySQL
        connection = connect_to_mysql()
        cursor = connection.cursor(dictionary=True)

        # Step 2: Add the speed_gps_avg column if it does not exist
        cursor.execute("""
            SELECT COUNT(*)
            FROM information_schema.columns
            WHERE table_name = 'preprocessed_driving_data'
            AND table_schema = %s
            AND column_name = 'speed_gps_avg'
        """, (MYSQL_DATABASE,))
        if cursor.fetchone()['COUNT(*)'] == 0:
            # Add the speed_gps_avg column
            cursor.execute("""
                ALTER TABLE preprocessed_driving_data
                ADD COLUMN speed_gps_avg DOUBLE NULL
            """)
            connection.commit()

        # Step 3: Fetch all data for the trip_id, ordered by timestamp
        cursor.execute("""
            SELECT timestamp, speed_gps
            FROM preprocessed_driving_data
            WHERE trip_id = %s
            ORDER BY timestamp
        """, (trip_id,))
        data = cursor.fetchall()

        if not data:
            print(f"No data found for trip_id: {trip_id}.")
            return

        # Step 4: Calculate the centered moving average
        moving_averages = [None] * len(data)
        for i in range(len(data)):
            # Use the previous, current, and next rows
            values = []
            if i > 0 and data[i - 1]['speed_gps'] is not None:
                values.append(data[i - 1]['speed_gps'])
            if data[i]['speed_gps'] is not None:
                values.append(data[i]['speed_gps'])
            if i < len(data) - 1 and data[i + 1]['speed_gps'] is not None:
                values.append(data[i + 1]['speed_gps'])

            # Calculate the average if there are valid values
            if values:
                moving_averages[i] = sum(values) / len(values)

        # Step 5: Update the database with calculated moving averages
        for i, row in enumerate(data):
            cursor.execute("""
                UPDATE preprocessed_driving_data
                SET speed_gps_avg = %s
                WHERE trip_id = %s AND timestamp = %s
            """, (moving_averages[i], trip_id, row['timestamp']))

        connection.commit()

    except Error as e:
        print(f"Error: {e}")
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()

def calculate_acceleration_with_avg_speed(trip_id):
    """
    Calculate acceleration (miles per second squared) using the speed_gps_avg column
    and save it in the acceleration column.
    """
    connection = None
    try:
        # Step 1: Connect to MySQL
        connection = connect_to_mysql()
        cursor = connection.cursor(dictionary=True)

        # Step 2: Add the acceleration column if it does not exist
        cursor.execute("""
            SELECT COUNT(*)
            FROM information_schema.columns
            WHERE table_name = 'preprocessed_driving_data'
            AND table_schema = %s
            AND column_name = 'acceleration'
        """, (MYSQL_DATABASE,))
        if cursor.fetchone()['COUNT(*)'] == 0:
            # Add the acceleration column
            cursor.execute("""
                ALTER TABLE preprocessed_driving_data
                ADD COLUMN acceleration DOUBLE NULL
            """)
            connection.commit()

        # Step 3: Fetch all data for the trip_id, ordered by timestamp
        cursor.execute("""
            SELECT timestamp, speed_gps_avg
            FROM preprocessed_driving_data
            WHERE trip_id = %s
            ORDER BY timestamp
        """, (trip_id,))
        data = cursor.fetchall()

        if not data:
            print(f"No data found for trip_id: {trip_id}.")
            return

        # Step 4: Calculate acceleration for each row
        accelerations = [None] * len(data)
        for i in range(1, len(data)):
            speed1 = data[i - 1]['speed_gps_avg']  # Speed in mph
            speed2 = data[i]['speed_gps_avg']      # Speed in mph

            # Convert speed from mph to miles per second
            speed1_mps = speed1 / 3600 if speed1 is not None else None
            speed2_mps = speed2 / 3600 if speed2 is not None else None

            # Calculate time difference in seconds
            time1 = datetime.strptime(data[i - 1]['timestamp'], "%Y-%m-%dT%H:%M:%S.%f")
            time2 = datetime.strptime(data[i]['timestamp'], "%Y-%m-%dT%H:%M:%S.%f")
            time_diff = (time2 - time1).total_seconds()

            # Calculate acceleration
            if speed1_mps is not None and speed2_mps is not None and time_diff > 0:
                accelerations[i] = (speed2_mps - speed1_mps) / time_diff

        # Step 5: Update the database with calculated acceleration values
        for i, row in enumerate(data):
            cursor.execute("""
                UPDATE preprocessed_driving_data
                SET acceleration = %s
                WHERE trip_id = %s AND timestamp = %s
            """, (accelerations[i], trip_id, row['timestamp']))

        connection.commit()

    except Error as e:
        print(f"Error: {e}")
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()


def calculate_lateral_acceleration(trip_id):
    """
    Calculate lateral acceleration using bearing_gps_avg and speed_gps_avg,
    considering 360-degree wrapping. Save the results in lateral_acceleration
    column.
    """
    connection = None
    try:
        # Connect to MySQL
        connection = connect_to_mysql()
        cursor = connection.cursor(dictionary=True)

        # Add the lateral_acceleration column if it doesn't exist
        cursor.execute("""
            SELECT COUNT(*)
            FROM information_schema.columns
            WHERE table_name = 'preprocessed_driving_data'
            AND table_schema = %s
            AND column_name = 'lateral_acceleration'
        """, (MYSQL_DATABASE,))
        
        if cursor.fetchone()['COUNT(*)'] == 0:
            cursor.execute("""
                ALTER TABLE preprocessed_driving_data
                ADD COLUMN lateral_acceleration DOUBLE NULL
            """)
            connection.commit()

        # Fetch all data for the trip_id ordered by timestamp
        cursor.execute("""
            SELECT timestamp, speed_gps_avg, bearing_gps_avg
            FROM preprocessed_driving_data
            WHERE trip_id = %s
            ORDER BY timestamp
        """, (trip_id,))
        data = cursor.fetchall()

        if not data:
            print(f"No data found for trip_id: {trip_id}.")
            return

        # Calculate lateral acceleration
        lateral_acceleration = [None] * len(data)
        g_force_constant = 9.80665  # m/s^2

        for i in range(1, len(data)):
            speed1 = data[i - 1]['speed_gps_avg']
            speed2 = data[i]['speed_gps_avg']
            bearing1 = data[i - 1]['bearing_gps_avg']
            bearing2 = data[i]['bearing_gps_avg']

            if None in (speed1, speed2, bearing1, bearing2):
                continue

            # Convert speed from mph to m/s
            speed1_mps = speed1 * 0.44704
            speed2_mps = speed2 * 0.44704

            # Calculate bearing difference, accounting for 360-degree wrapping
            delta_bearing = (bearing2 - bearing1 + 180) % 360 - 180

            # Calculate lateral acceleration (a = v^2 * tan(delta_bearing) / g)
            # Use the average speed between the two points
            avg_speed_mps = (speed1_mps + speed2_mps) / 2
            angular_velocity_rad = math.radians(delta_bearing)

            lateral_acceleration[i] = avg_speed_mps**2 * math.tan(angular_velocity_rad) / g_force_constant

        # Update the database with calculated lateral acceleration
        for i, row in enumerate(data):
            cursor.execute("""
                UPDATE preprocessed_driving_data
                SET lateral_acceleration = %s
                WHERE trip_id = %s AND timestamp = %s
            """, (lateral_acceleration[i], trip_id, row['timestamp']))

        connection.commit()

    except Error as e:
        print(f"Error: {e}")
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()

def calculate_lateral_acceleration_moving_average(trip_id):
    """
    Calculate a centered moving average for lateral_acceleration
    with a 3-second window and save it in the lateral_acceleration_avg column.
    """
    connection = None
    try:
        # Connect to MySQL
        connection = connect_to_mysql()
        cursor = connection.cursor(dictionary=True)

        # Add the lateral_acceleration_avg column if it doesn't exist
        cursor.execute("""
            SELECT COUNT(*)
            FROM information_schema.columns
            WHERE table_name = 'preprocessed_driving_data'
            AND table_schema = %s
            AND column_name = 'lateral_acceleration_avg'
        """, (MYSQL_DATABASE,))
        
        if cursor.fetchone()['COUNT(*)'] == 0:
            cursor.execute("""
                ALTER TABLE preprocessed_driving_data
                ADD COLUMN lateral_acceleration_avg DOUBLE NULL
            """)
            connection.commit()

        # Fetch all data for the trip_id ordered by timestamp
        cursor.execute("""
            SELECT timestamp, lateral_acceleration
            FROM preprocessed_driving_data
            WHERE trip_id = %s
            ORDER BY timestamp
        """, (trip_id,))
        data = cursor.fetchall()

        if not data:
            print(f"No data found for trip_id: {trip_id}.")
            return

        # Calculate the centered moving average
        moving_averages = [None] * len(data)
        for i in range(len(data)):
            values = []
            if i > 0 and data[i - 1]['lateral_acceleration'] is not None:
                values.append(data[i - 1]['lateral_acceleration'])
            if data[i]['lateral_acceleration'] is not None:
                values.append(data[i]['lateral_acceleration'])
            if i < len(data) - 1 and data[i + 1]['lateral_acceleration'] is not None:
                values.append(data[i + 1]['lateral_acceleration'])

            if values:
                moving_averages[i] = sum(values) / len(values)

        # Update the database with calculated moving averages
        for i, row in enumerate(data):
            cursor.execute("""
                UPDATE preprocessed_driving_data
                SET lateral_acceleration_avg = %s
                WHERE trip_id = %s AND timestamp = %s
            """, (moving_averages[i], trip_id, row['timestamp']))

        connection.commit()

    except Error as e:
        print(f"Error: {e}")
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()

def preprocess_trip_data(trip_id):
    preprocess_timestamp_data(trip_id)
    interpolate_lat_lon(trip_id)
    calculate_bearing(trip_id)
    calculate_moving_average(trip_id)
    calculate_speed(trip_id)
    calculate_speed_moving_average(trip_id)
    calculate_acceleration_with_avg_speed(trip_id)
    calculate_lateral_acceleration(trip_id)
    calculate_lateral_acceleration_moving_average(trip_id)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit(1)

    trip_id = sys.argv[1]

    preprocess_trip_data(trip_id)
    print(f"Preprocessed data for trip_id {trip_id} saved in preprocessed_driving_data.")