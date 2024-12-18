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


def save_device_trip_mapping(trip_id, gps_device_id, sensor_device_id):
    """
    Save the mapping between trip_id, gps_device_id, and sensor_device_id to the device_trip_mapping table.
    """
    connection = connect_to_mysql()
    if not connection:
        return

    try:
        cursor = connection.cursor()

        # Create the device_trip_mapping table if it doesn't exist
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS device_trip_mapping (
                trip_id VARCHAR(255) PRIMARY KEY,
                gps_device_id VARCHAR(255),
                sensor_device_id VARCHAR(255),
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (gps_device_id, sensor_device_id)
                    REFERENCES user_info(gps_device_id, sensor_device_id)
                    ON DELETE CASCADE
            )
        ''')

        # Insert or update the device-trip mapping
        cursor.execute('''
            INSERT INTO device_trip_mapping (trip_id, gps_device_id, sensor_device_id)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE 
                gps_device_id = VALUES(gps_device_id),
                sensor_device_id = VALUES(sensor_device_id)
        ''', (trip_id, gps_device_id, sensor_device_id))

        connection.commit()
        print(f"Device-trip mapping saved: Trip ID = {trip_id}, GPS Device ID = {gps_device_id}, Sensor Device ID = {sensor_device_id}")
    except Error as e:
        print(f"Error saving device-trip mapping: {e}")
    finally:
        connection.close()


if __name__ == "__main__":
    # Accept the trip_id, gps_device_id, and sensor_device_id as command-line arguments or prompt for input
    if len(sys.argv) > 3:
        trip_id = sys.argv[1]
        gps_device_id = sys.argv[2]
        sensor_device_id = sys.argv[3]
    else:
        print("Error: Please provide the trip_id, gps_device_id and the sensor_device_id as a command-line argument.")
        sys.exit(1)

    # Save the device-trip mapping
    save_device_trip_mapping(trip_id, gps_device_id, sensor_device_id)