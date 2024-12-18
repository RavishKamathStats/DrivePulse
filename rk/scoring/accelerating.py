import sys
import os

# Add the parent directory of `testing_user` to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from rk.sql.database import connect_to_mysql


# Create or update required tables
def create_tables(connection):
    try:
        cursor = connection.cursor()

        # Create or update scores_data table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS scores_data (
            trip_id VARCHAR(255) PRIMARY KEY,
            acceleration_score FLOAT DEFAULT 100,
            braking_score FLOAT DEFAULT 100,
            speeding_score FLOAT DEFAULT 100,
            cornering_score FLOAT DEFAULT 100
        )
        """)

        # Create or update penalty_events_data table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS penalty_events_data (
            trip_id VARCHAR(255),
            timestamp VARCHAR(255),
            latitude DOUBLE,
            longitude DOUBLE,
            acceleration_event VARCHAR(10),
            braking_event VARCHAR(10),
            speeding_event VARCHAR(10),
            cornering_event VARCHAR(10),
            PRIMARY KEY (trip_id, timestamp)
        )
        """)

        connection.commit()
        # print("Tables created successfully.")

    except Exception as e:
        print(f"Error creating tables: {e}")
        sys.exit(1)


# Fetch acceleration data for a specific trip
def get_acceleration_data(trip_id, connection):
    try:
        cursor = connection.cursor()
        query = """
        SELECT trip_id, timestamp, latitude, longitude, acceleration
        FROM preprocessed_driving_data
        WHERE trip_id = %s
        """
        cursor.execute(query, (trip_id,))
        return cursor.fetchall()
    except Exception as e:
        print(f"Error fetching data: {e}")
        sys.exit(1)


# Convert acceleration from mps² to G-force
def convert_to_g_force(acceleration_mps2):
    return (acceleration_mps2 * 1609.34) / 9.80665


# Score acceleration and track penalty events
def score_acceleration(data, connection):
    trip_duration = len(data)

    if trip_duration == 0:
        print("No data found for scoring.")
        return 0

    try:
        cursor = connection.cursor()
        total_score = 0
        penalty_count = 0  # Count of penalty events

        for record in data:
            trip_id, timestamp, latitude, longitude, acceleration_mps2 = record

            # Skip rows where acceleration is None
            if acceleration_mps2 is None:
                continue

            # Convert acceleration to G-force
            acceleration_g = convert_to_g_force(acceleration_mps2)

            # Determine if a penalty applies for acceleration
            if 0.15 <= acceleration_g < 0.25:
                acceleration_event = "mild"
                penalty_score = 5
            elif acceleration_g >= 0.25:
                acceleration_event = "severe"
                penalty_score = 10
            else:
                # Insert row even if there's no penalty (NULL event)
                acceleration_event = None
                penalty_score = 0

            # Corrected INSERT statement for penalty_events_data
            cursor.execute("""
            INSERT INTO penalty_events_data (
                trip_id, timestamp, latitude, longitude, acceleration_event
            ) 
            VALUES (%s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE 
                acceleration_event = VALUES(acceleration_event)
            """, (trip_id, timestamp, latitude, longitude, acceleration_event))

            # Update total score only if a penalty occurred
            if penalty_score > 0:
                total_score += penalty_score
                penalty_count += 1

        # Calculate the normalized acceleration score
        if penalty_count == 0:
            normalized_score = 100.0  # Perfect score if no penalties
        else:
            normalized_score = max(10, 100 - (total_score / trip_duration * 100))

        # Insert or update the acceleration score in scores_data
        cursor.execute("""
        INSERT INTO scores_data (trip_id, acceleration_score, braking_score, speeding_score)
        VALUES (%s, %s, NULL, NULL)
        ON DUPLICATE KEY UPDATE acceleration_score = %s
        """, (trip_id, normalized_score, normalized_score))

        connection.commit()
        print(f"Acceleration scores updated for trip_id {trip_id}. Final acceleration score: {normalized_score:.2f}")
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
        # Create the required tables
        create_tables(connection)

        # Fetch and score data
        accel_data = get_acceleration_data(trip_id, connection)
        score_acceleration(accel_data, connection)

    finally:
        if connection.is_connected():
            connection.close()
