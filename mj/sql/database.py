import mysql.connector
import sys

# MySQL Configuration
MYSQL_HOST = 'ec2-54-241-86-221.us-west-1.compute.amazonaws.com'
MYSQL_USER = 'driving_user'
MYSQL_PASSWORD = 'dsci560'
MYSQL_DATABASE = 'driving_data'


# Connect to MySQL
def connect_to_mysql():
    """
    Establish a connection to the MySQL database.
    """
    try:
        connection = mysql.connector.connect(
            host=MYSQL_HOST,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DATABASE,
            auth_plugin='mysql_native_password'
        )
        if connection.is_connected():
            return connection
    except mysql.connector.Error as e:
        print(f"Error connecting to MySQL: {e}")
        sys.exit(1)
