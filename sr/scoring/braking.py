import sys
import os

# Add the parent directory of `testing_user` to sys.path
sys.path.append(
    os.path.abspath(
        os.path.join(os.path.dirname(__file__), "../..")
    )
)

from sr.sql.database import connect_to_mysql


# Check if a column exists in a table
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


# Update the Database Schema
def update_tables(connection):
    try:
        cursor = connection.cursor()

        # Check and add `braking_event` to `penalty_events_data`
        if not column_exists(connection, "penalty_events_data", "braking_event"):
            cursor.execute("""
            ALTER TABLE penalty_events_data 
            ADD COLUMN braking_event VARCHAR(10)
            """)
            print("Added column `braking_event` to `penalty_events_data`.")

        # Check and add `braking_score` to `scores_data`
        if not column_exists(connection, "scores_data", "braking_score"):
            cursor.execute("""
            ALTER TABLE scores_data 
            ADD COLUMN braking_score FLOAT
            """)
            print("Added column `braking_score` to `scores_data`.")

        connection.commit()
    
    except Exception as e:
        print(f"Error updating tables: {e}")
        sys.exit(1)


# Fetch braking data for a specific trip
def get_braking_data(trip_id, connection):
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


# Score braking and track penalty events
def score_braking(data, connection):
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

            # Determine if a penalty applies for braking
            if -0.25 <= acceleration_g < -0.15:
                braking_event = "mild"
                penalty_score = 5
            elif acceleration_g < -0.25:
                braking_event = "severe"
                penalty_score = 10
            else:
                # No penalty
                braking_event = None
                penalty_score = 0

            # Corrected INSERT statement for penalty_events_data
            cursor.execute("""
            INSERT INTO penalty_events_data (
                trip_id, timestamp, latitude, longitude, braking_event
            ) 
            VALUES (%s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE 
                braking_event = VALUES(braking_event)
            """, (trip_id, timestamp, latitude, longitude, braking_event))

            # Update total score if a penalty occurred
            if penalty_score > 0:
                total_score += penalty_score
                penalty_count += 1

        # Calculate normalized braking score
        if penalty_count == 0:
            normalized_score = 100.0  # Perfect score if no penalties
        else:
            normalized_score = max(10, 100 - (total_score / trip_duration * 100))

        # Insert or update the braking score in scores_data
        cursor.execute("""
        INSERT INTO scores_data (trip_id, acceleration_score, braking_score, speeding_score)
        VALUES (%s, NULL, %s, NULL)
        ON DUPLICATE KEY UPDATE braking_score = %s
        """, (trip_id, normalized_score, normalized_score))

        connection.commit()
        print(f"Braking scores updated for trip_id {trip_id}. Final braking score: {normalized_score:.2f}")
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
        # Update Tables Before Scoring
        update_tables(connection)

        # Fetch and score data
        braking_data = get_braking_data(trip_id, connection)
        score_braking(braking_data, connection)

    finally:
        if connection.is_connected():
            connection.close()

