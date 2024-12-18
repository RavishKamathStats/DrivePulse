import sys
import os

# Add the parent directory of `testing_user` to sys.path
sys.path.append(
    os.path.abspath(
        os.path.join(os.path.dirname(__file__), "../..")
    )
)

from mj.sql.database import connect_to_mysql


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


# Update tables to add cornering columns
def update_tables(connection):
    try:
        cursor = connection.cursor()

        # Add columns if they don't exist
        if not column_exists(connection, "penalty_events_data", "cornering_event"):
            cursor.execute("""
            ALTER TABLE penalty_events_data 
            ADD COLUMN cornering_event VARCHAR(10)
            """)
            print("Added column `cornering_event` to `penalty_events_data`.")

        if not column_exists(connection, "scores_data", "cornering_score"):
            cursor.execute("""
            ALTER TABLE scores_data
            ADD COLUMN cornering_score FLOAT
            """)
            print("Added column `cornering_score` to `scores_data`.")

        connection.commit()

    except Exception as e:
        print(f"Error updating tables: {e}")
        sys.exit(1)


# Fetch cornering data for a specific trip
def get_cornering_data(trip_id, connection):
    try:
        cursor = connection.cursor()
        query = """
        SELECT trip_id, timestamp, latitude, longitude, lateral_acceleration_avg
        FROM preprocessed_driving_data
        WHERE trip_id = %s
        ORDER BY timestamp
        """
        cursor.execute(query, (trip_id,))
        return cursor.fetchall()
    except Exception as e:
        print(f"Error fetching data: {e}")
        sys.exit(1)


# Score cornering and track penalty events
def score_cornering(data, connection):
    trip_duration = len(data)

    if trip_duration == 0:
        print("No data found for scoring.")
        return 0

    try:
        cursor = connection.cursor()
        total_score = 0
        penalty_count = 0

        for record in data:
            trip_id, timestamp, lat, lon, lateral_accel = record

            # Skip if lateral acceleration is missing
            if lateral_accel is None:
                continue

            # Take the absolute value of lateral acceleration
            lateral_accel = abs(lateral_accel)

            # Determine if a penalty applies for cornering
            if 0.18 <= lateral_accel < 0.35:
                cornering_event = "mild"
                penalty_score = 5
            elif lateral_accel >= 0.35:
                cornering_event = "severe"
                penalty_score = 10
            else:
                cornering_event = None
                penalty_score = 0

            # Corrected INSERT statement for penalty_events_data
            cursor.execute("""
            INSERT INTO penalty_events_data (
                trip_id, timestamp, latitude, longitude, cornering_event
            ) 
            VALUES (%s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE 
                cornering_event = VALUES(cornering_event)
            """, (trip_id, timestamp, lat, lon, cornering_event))

            # Update total score if a penalty occurred
            if penalty_score > 0:
                total_score += penalty_score
                penalty_count += 1

        # Calculate normalized cornering score
        if penalty_count == 0:
            normalized_score = 100.0  # Perfect score if no penalties
        else:
            normalized_score = max(10, 100 - (total_score / trip_duration * 100))

        # Insert or update the cornering score in scores_data
        cursor.execute("""
        INSERT INTO scores_data (trip_id, cornering_score)
        VALUES (%s, %s)
        ON DUPLICATE KEY UPDATE cornering_score = %s
        """, (trip_id, normalized_score, normalized_score))

        connection.commit()
        print(f"Cornering scores updated for trip_id {trip_id}. Final cornering score: {normalized_score:.2f}")
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
        cornering_data = get_cornering_data(trip_id, connection)
        score_cornering(cornering_data, connection)

    finally:
        if connection.is_connected():
            connection.close()