import sys
import os

# Add the parent directory of `testing_user` to sys.path
sys.path.append(
    os.path.abspath(
        os.path.join(os.path.dirname(__file__), "../..")
    )
)

from sql.database import connect_to_mysql
from mysql.connector import Error


def ensure_user_info(gps_device_id, sensor_device_id):
    """
    Ensure user information exists in the user_info table.
    """
    connection = connect_to_mysql()
    if not connection:
        return

    try:
        cursor = connection.cursor()

        # Create the user_info table if it doesn't exist
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS user_info (
                gps_device_id VARCHAR(255),
                sensor_device_id VARCHAR(255),
                name VARCHAR(255),
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (gps_device_id, sensor_device_id)
            )
        ''')

        # Check if the user exists
        cursor.execute('''
            SELECT name
            FROM user_info
            WHERE gps_device_id = %s AND sensor_device_id = %s
        ''', (gps_device_id, sensor_device_id))
        result = cursor.fetchone()

        if result:
            print(f"User already exists: GPS Device ID = {gps_device_id}, Sensor Device ID = {sensor_device_id}, Name = {result[0]}")
        else:
            user_name = input(f"Enter the name for GPS Device ID {gps_device_id} and Sensor Device ID {sensor_device_id}: ").strip()
            cursor.execute('''
                INSERT INTO user_info (gps_device_id, sensor_device_id, name) 
                VALUES (%s, %s, %s)
            ''', (gps_device_id, sensor_device_id, user_name))
            connection.commit()
            print(f"New user added: GPS Device ID = {gps_device_id}, Sensor Device ID = {sensor_device_id}, Name = {user_name}")
    except Error as e:
        print(f"Error ensuring user info: {e}")
    finally:
        connection.close()


if __name__ == "__main__":
    # Accept the gps_device_id and sensor_device_id as command-line arguments
    if len(sys.argv) > 2:
        gps_device_id = sys.argv[1]
        sensor_device_id = sys.argv[2]
    else:
        print("Error: Please provide the GPS and Sensor Device IDs as command-line arguments.")
        sys.exit(1)

    # Ensure user information exists
    ensure_user_info(gps_device_id, sensor_device_id)