import sys
import os
import osmnx as ox
from geopy.distance import geodesic

# Add the parent directory of `testing_user` to sys.path
sys.path.append(
    os.path.abspath(
        os.path.join(os.path.dirname(__file__), "../..")
    )
)

from testing_user.sql.database import connect_to_mysql

# Speed limit cache for known GPS points
speed_limit_cache = {}


# Check if columns exist
def column_exists(connection, table_name, column_name):
    try:
        cursor = connection.cursor()
        query = """
        SELECT COUNT(*)
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s AND COLUMN_NAME = %s
        """
        cursor.execute(query, (table_name, column_name))
        return cursor.fetchone()[0] == 1
    except Exception as e:
        print(f"Error checking column existence: {e}")
        sys.exit(1)


# Update the tables to add columns
def update_tables(connection):
    try:
        cursor = connection.cursor()

        # Add columns if they don't exist
        if not column_exists(connection, "penalty_events_data", "speeding_event"):
            cursor.execute("""
            ALTER TABLE penalty_events_data 
            ADD COLUMN speeding_event VARCHAR(10)
            """)
            # print("Added column `speeding_event` to `penalty_events_data`.")

        if not column_exists(connection, "scores_data", "speeding_score"):
            cursor.execute("""
            ALTER TABLE scores_data 
            ADD COLUMN speeding_score FLOAT
            """)
            # print("Added column `speeding_score` to `scores_data`.")

        connection.commit()
        # print("Database schema updated successfully.")

    except Exception as e:
        print(f"Error updating tables: {e}")
        sys.exit(1)


# Fetch speeding data
def get_speeding_data(trip_id, connection):
    try:
        cursor = connection.cursor()
        query = """
        SELECT trip_id, timestamp, latitude, longitude, speed_gps_avg
        FROM preprocessed_driving_data
        WHERE trip_id = %s
        """
        cursor.execute(query, (trip_id,))
        return cursor.fetchall()
    except Exception as e:
        print(f"Error fetching data: {e}")
        sys.exit(1)


# Fetch the speed limit from OpenStreetMap
def get_speed_limit(lat, lon):
    try:
        # Search for roads near the GPS point
        location = (lat, lon)
        tags = {'highway': True}  # Extract only roads
        gdf = ox.features_from_point(location, tags, dist=50)

        # Filter to include only speed limit information
        if 'maxspeed' in gdf.columns:
            max_speed = gdf['maxspeed'].dropna().iloc[0]

            # Ensure speed is numeric
            if isinstance(max_speed, str):
                if "mph" in max_speed:
                    max_speed = float(max_speed.replace(" mph", ""))
                elif max_speed.isdigit():
                    max_speed = float(max_speed)

            # Cache the result
            speed_limit_cache[(lat, lon)] = max_speed
            return max_speed
        # print(f"No speed limit data found for point ({lat}, {lon}).")
        return None

    except Exception as e:
        print(f"Error fetching speed limit at ({lat}, {lon}): {e}")
        return None


# Find the nearest GPS point with a known speed limit
def find_nearest_speed_limit(lat, lon):
    try:
        if not speed_limit_cache:
            print("Speed limit cache is empty. No nearest point found.")
            return None

        # Find the nearest point
        nearest_point = min(
            speed_limit_cache.keys(),
            key=lambda point: geodesic((lat, lon), point).miles
        )

        # Get the distance to the nearest point
        distance = geodesic((lat, lon), nearest_point).miles
        nearest_speed_limit = speed_limit_cache[nearest_point]

        # print(f"Nearest speed limit found: {nearest_speed_limit} mph at {nearest_point} "
        #       f"({distance:.2f} miles away from {lat}, {lon}).")

        return nearest_speed_limit

    except ValueError:
        print("No speed limit data available.")
        return None


# Score speeding and track penalty events
def score_speeding(data, connection):
    trip_duration = len(data)

    if trip_duration == 0:
        print("No data found for scoring.")
        return 0

    try:
        cursor = connection.cursor()
        total_score = 0
        penalty_count = 0  # Count of penalty events

        for record in data:
            trip_id, timestamp, latitude, longitude, speed_gps_avg = record

            # Skip rows where speed is None
            if speed_gps_avg is None:
                print(f"Skipping row at {timestamp} (missing speed data).")
                continue

            # Fetch the speed limit from OpenStreetMap
            speed_limit = get_speed_limit(latitude, longitude)

            if speed_limit is None:
                # print(f"Speed limit not found at {latitude}, {longitude}.
                # Looking for nearest point.")
                speed_limit = find_nearest_speed_limit(latitude, longitude)

            if speed_limit is None:
                print(f"No nearest speed limit found for row at {timestamp}. Skipping.")
                continue

            # Determine if a penalty applies for speeding
            over_speed_percentage = (speed_gps_avg - speed_limit) / speed_limit * 100

            if 5 <= over_speed_percentage <= 10:
                speeding_event = "mild"
                penalty_score = 5
            elif over_speed_percentage > 10:
                speeding_event = "severe"
                penalty_score = 10
            else:
                continue  # No penalty for safe driving

            # INSERT or UPDATE for speeding_event
            cursor.execute("""
            INSERT INTO penalty_events_data (
                trip_id, timestamp, latitude, longitude, speeding_event
            ) 
            VALUES (%s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                speeding_event = VALUES(speeding_event)
            """, (trip_id, timestamp, latitude, longitude, speeding_event))

            # Update total score
            total_score += penalty_score
            penalty_count += 1

        # Calculate the normalized speeding score
        if penalty_count == 0:
            normalized_score = 100.0  # Perfect score if no penalties
        else:
            normalized_score = max(0, 100 - (total_score / trip_duration * 100))

        # Insert or update the speeding score in scores_data
        cursor.execute("""
        INSERT INTO scores_data (trip_id, acceleration_score, braking_score, speeding_score)
        VALUES (%s, NULL, NULL, %s)
        ON DUPLICATE KEY UPDATE speeding_score = %s
        """, (trip_id, normalized_score, normalized_score))

        connection.commit()
        print(f"Speeding scores updated for trip_id {trip_id}. Final speeding score: {normalized_score:.2f}")
        return normalized_score

    except Exception as e:
        print(f"Error scoring data: {e}")
        sys.exit(1)


# Main Execution
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python accelerating.py <trip_id>")
        sys.exit(1)

    trip_id = sys.argv[1]

    connection = connect_to_mysql()

    try:
        # Update tables before scoring
        update_tables(connection)

        # Fetch and score data
        speeding_data = get_speeding_data(trip_id, connection)
        score_speeding(speeding_data, connection)

    finally:
        if connection.is_connected():
            connection.close()
