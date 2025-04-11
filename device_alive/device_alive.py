import time
import datetime
import pyodbc
from influxdb import InfluxDBClient
from dotenv import load_dotenv
import os
import logging
import logging.handlers  # เพิ่มสำหรับ RotatingFileHandler

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Error logger
error_logger = logging.getLogger('error_logger')
error_handler = logging.handlers.RotatingFileHandler(
    'error.log', maxBytes=50*1024*1024, backupCount=5  # 50MB ต่อไฟล์, เก็บไฟล์เก่าสูงสุด 5 ไฟล์
)
error_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
error_logger.addHandler(error_handler)
error_logger.setLevel(logging.ERROR)

# Success logger
success_logger = logging.getLogger('success_logger')
success_handler = logging.handlers.RotatingFileHandler(
    'success.log', maxBytes=50*1024*1024, backupCount=5  # 50MB ต่อไฟล์, เก็บไฟล์เก่าสูงสุด 5 ไฟล์
)
success_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
success_logger.addHandler(success_handler)
success_logger.setLevel(logging.INFO)

# Device logger
device_logger = logging.getLogger('device_logger')
device_handler = logging.handlers.RotatingFileHandler(
    'log_device.log', maxBytes=10*1024*1024, backupCount=5  # 10MB ต่อไฟล์, เก็บไฟล์เก่าสูงสุด 5 ไฟล์
)
device_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
device_logger.addHandler(device_handler)
device_logger.setLevel(logging.DEBUG)

# โหลดค่าจากไฟล์ .env
load_dotenv()

# InfluxDB client
influx_client = None
# Load environment variables
INFLUXDB_HOST = os.getenv('INFLUXDB_HOST', 'localhost')
INFLUXDB_PORT = int(os.getenv('INFLUXDB_PORT', 8086))
INFLUXDB_DATABASE = os.getenv('INFLUXDB_DATABASE', 'test_db')

MSSQL_SERVER = os.getenv('MSSQL_SERVER', '192.168.0.128')
MSSQL_USER = os.getenv('MSSQL_USER', 'sa')
MSSQL_PASSWORD = os.getenv('MSSQL_PASSWORD', 'sa@admin')
MSSQL_DATABASE = os.getenv('MSSQL_DATABASE', 'iot_db')
MSSQL_PORT = int(os.getenv('MSSQL_PORT', 1433))

INTERVAL = int(os.getenv('INTERVAL', 1))

def connect_influxdb():
    global influx_client
    try:
        influx_client = InfluxDBClient(
            host=INFLUXDB_HOST,
            port=INFLUXDB_PORT,
            database=INFLUXDB_DATABASE
        )
        influx_client.ping()  # Test connection
        print("Connected to InfluxDB")
        return True
    except Exception as e:
        error_msg = f"[connect_influxdb] InfluxDB connection failed: {str(e)}"
        print(error_msg)
        error_logger.error(error_msg)
        influx_client = None
        return False

def connect_mssql():
    try:
        # Try different ODBC drivers in order of preference
        drivers = [
            "ODBC Driver 18 for SQL Server",
            "ODBC Driver 17 for SQL Server",
            "ODBC Driver 13 for SQL Server",
            "SQL Server"
        ]
        
        for driver in drivers:
            try:
                conn_str = (
                    f"DRIVER={{{driver}}};"
                    f"SERVER={MSSQL_SERVER};"
                    f"DATABASE={MSSQL_DATABASE};"
                    f"UID={MSSQL_USER};"
                    f"PWD={MSSQL_PASSWORD};"
                    f"PORT={MSSQL_PORT}"
                )
                conn = pyodbc.connect(conn_str)
                return conn
            except pyodbc.Error as e:
                continue
        
        error_msg = f"[connect_mssql] No suitable ODBC driver found"
        error_logger.error(error_msg)
        raise Exception(error_msg)
    
    except Exception as e:
        error_msg = f"[connect_mssql] MSSQL connection failed: {str(e)}"
        print(error_msg)
        error_logger.error(error_msg)
        return None

def get_topic_from_mssql():
    try:
        # Connect to MSSQL
        conn = connect_mssql()
        if not conn:
            error_msg = "[get_topic_from_mssql] Failed to connect to MSSQL"
            print(error_msg)
            error_logger.error(error_msg)
            return []

        cursor = conn.cursor()
        
        # Query distinct tools, machine, process, and location from device_master_tb
        query = """
            SELECT DISTINCT tools, machine, process, location 
            FROM device_master_tb 
            WHERE tools IS NOT NULL AND machine IS NOT NULL
        """
        cursor.execute(query)
        
        # Create a list of dictionaries with topic, process, and location
        topic_list = [
            {
                'topic': f"iot_sensors/device_alive/{row[0]}_{row[1]}",
                'iot_topic': f"iot_sensors/iot_{row[0]}/{row[1]}",
                'process': row[2],
                'location': row[3]
            }
            for row in cursor.fetchall()
            if row[0] and row[1]
        ]
        
        # Close connection
        cursor.close()
        conn.close()
        
        # Return sorted unique topic dictionaries
        return sorted(topic_list, key=lambda x: x['topic'])
    except Exception as e:
        error_msg = f"[get_topic_from_mssql] Error: {str(e)}"
        print(error_msg)
        error_logger.error(error_msg)
        return []

def fetch_influxdb_data(topic_list):
    global influx_client
    result_data = {}
    
    try:
        if not influx_client:
            error_msg = "[fetch_influxdb_data] No InfluxDB connection"
            error_logger.error(error_msg)
            print(error_msg)
            return result_data

        for topic_dict in topic_list:
            topic = topic_dict['topic']
            iot_topic = topic_dict['iot_topic']
            process = topic_dict['process']
            location = topic_dict['location']
            
            try:
                # Extract measurement name from topic
                measurement = 'device_alive'
                print("topic:", topic)
                # Query InfluxDB for the latest data point
                query = f'SELECT * FROM "{measurement}" WHERE topic = \'{topic}\' and time > now() - 1m ORDER BY time DESC LIMIT 1'
                result = influx_client.query(query)

                # Check if result contains data
                if result:
                    data_points = list(result[measurement])
                    result_data[topic] = data_points
                    
                    # Log success for topic with data
                    # success_msg = f"[fetch_influxdb_data] Successfully fetched data for topic: {topic}, process: {process}, location: {location}"
                    # success_logger.info(success_msg)
                    # print(success_msg)
                else:
                    # Log empty result for topic
                    device_msg = f"[fetch_influxdb_data] Device disconnected for topic: {topic}, process: {process}, location: {location}"
                    device_logger.debug(device_msg)
                    # print(device_msg)
                    
            except Exception as e:
                error_msg = f"[fetch_influxdb_data] Error fetching data for topic {topic}: {str(e)}"
                error_logger.error(error_msg)
                print(error_msg)
                
    except Exception as e:
        error_msg = f"[fetch_influxdb_data] General error: {str(e)}"
        error_logger.error(error_msg)
        print(error_msg)
        
    return result_data

def main():
    while True:
        try:
            start_time = time.time()
            if not connect_influxdb():
                time.sleep(1)
                continue
            topic_list = get_topic_from_mssql()
            if not topic_list:
                print("No matching topic found")
                time.sleep(5)
                continue
                
            influx_data = fetch_influxdb_data(topic_list)
            elapsed_time = time.time() - start_time
            remaining_time = max(0, INTERVAL*60 - elapsed_time)
            time.sleep(remaining_time)
            print("using_time: ", time.time() - start_time)
        except Exception as e:
            error_msg = f"[main] Main error: {str(e)}"
            print(error_msg)
            error_logger.error(error_msg)

if __name__ == "__main__":
    main()