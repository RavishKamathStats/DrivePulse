import sys
import os

# Add the parent directory of `testing_user` to sys.path
sys.path.append(
    os.path.abspath(
        os.path.join(os.path.dirname(__file__), "../..")
    )
)

from rk.sql.database import connect_to_mysql


# Fetch scores from the database
def get_component_scores(trip_id, connection):
    try:
        cursor = connection.cursor()
        query = """
        SELECT acceleration_score, braking_score, speeding_score, cornering_score
        FROM scores_data
        WHERE trip_id = %s
        """
        cursor.execute(query, (trip_id,))
        result = cursor.fetchone()
        if result:
            return result
        else:
            print(f"No scores found for trip_id {trip_id}.")
            sys.exit(1)
    except Exception as e:
        print(f"Error fetching scores: {e}")
        sys.exit(1)


# Calculate the final weighted score
def calculate_final_score(scores):
    acceleration_score, braking_score, speeding_score, cornering_score = scores

    # Define weights
    WEIGHTS = {
        "acceleration": 0.25,  # 25%
        "braking": 0.25,       # 25%
        "speeding": 0.30,      # 30%
        "cornering": 0.20      # 20%
    }

    # Calculate the final score
    final_score = (
        (acceleration_score * WEIGHTS["acceleration"]) +
        (braking_score * WEIGHTS["braking"]) +
        (speeding_score * WEIGHTS["speeding"]) +
        (cornering_score * WEIGHTS["cornering"])
    )
    return round(final_score, 2)


# Insert or update the final score in the database
def update_final_score(trip_id, final_score, connection):
    try:
        cursor = connection.cursor()

        # Add column if it doesn't exist
        cursor.execute("""
        SELECT COUNT(*)
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'scores_data' AND COLUMN_NAME = 'final_score'
        """)
        if cursor.fetchone()[0] == 0:
            cursor.execute("""
            ALTER TABLE scores_data 
            ADD COLUMN final_score FLOAT
            """)
            # print("Added column `final_score` to `scores_data`.")

        # Insert or update the final score
        cursor.execute("""
        UPDATE scores_data
        SET final_score = %s
        WHERE trip_id = %s
        """, (final_score, trip_id))

        connection.commit()
        print(f"Final score updated for trip_id {trip_id}. Final score: {final_score:.2f}")
    except Exception as e:
        print(f"Error updating final score: {e}")
        sys.exit(1)


# Main Execution
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python final_score.py <trip_id>")
        sys.exit(1)

    trip_id = sys.argv[1]
    connection = connect_to_mysql()

    try:
        # Fetch the component scores
        component_scores = get_component_scores(trip_id, connection)

        # Calculate and update the final score
        final_score = calculate_final_score(component_scores)
        update_final_score(trip_id, final_score, connection)

    finally:
        if connection.is_connected():
            connection.close()
        # print("Database connection closed.")