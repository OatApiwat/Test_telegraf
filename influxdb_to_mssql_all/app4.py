import os
import time
from influxdb import InfluxDBClient
import pymssql
from datetime import datetime

# Load environment variables
INFLUXDB_HOST = os.getenv('INFLUXDB_HOST', 'localhost')
INFLUXDB_PORT = int(os.getenv('INFLUXDB_PORT', 8086))
INFLUXDB_DATABASE = os.getenv('INFLUXDB_DATABASE', 'test_db')

MSSQL_SERVER = os.getenv('MSSQL_SERVER', '192.168.0.128')
MSSQL_USER = os.getenv('MSSQL_USER', 'sa')
MSSQL_PASSWORD = os.getenv('MSSQL_PASSWORD', 'sa@admin')
MSSQL_DATABASE = os.getenv('MSSQL_DATABASE', 'iot_db')
MSSQL_PORT = int(os.getenv('MSSQL_PORT', 1433))

INTERVAL = int(os.getenv('INTERVAL', 60))

MEASUREMENT_LIST = ['mesure_1', 'mesure_2', 'mesure_3', 'mesure_4']

# InfluxDB client
influx_client = None

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
        print(f"InfluxDB connection failed: {e}")
        influx_client = None
        return False

def get_mssql_connection():
    try:
        conn = pymssql.connect(
            server=MSSQL_SERVER,
            user=MSSQL_USER,
            password=MSSQL_PASSWORD,
            database=MSSQL_DATABASE,
            port=MSSQL_PORT
        )
        return conn
    except Exception as e:
        print(f"MSSQL connection failed: {e}")
        return None

def map_influx_to_mssql_type(influx_type):
    """Map InfluxDB data types to MSSQL data types"""
    if isinstance(influx_type, int):
        return "INT"
    elif isinstance(influx_type, float):
        return "FLOAT"
    elif isinstance(influx_type, str):
        return "NVARCHAR(255)"
    elif isinstance(influx_type, bool):
        return "BIT"
    return "NVARCHAR(255)"  # Default type

def create_table_auto(measurement):
    if not influx_client:
        return False
    
    try:
        # Get top 1 record from InfluxDB
        query = f'SELECT * FROM "{measurement}" LIMIT 1'
        result = influx_client.query(query)
        points = list(result.get_points())
        
        if not points:
            return False

        sample_point = points[0]
        table_name = f"{measurement}_tb"
        
        # Prepare columns
        columns = []
        columns.append("id BIGINT IDENTITY(1,1) PRIMARY KEY")
        columns.append("time DATETIME NOT NULL")
        
        for key, value in sample_point.items():
            if key != 'time':  # Skip time as it's already added
                sql_type = map_influx_to_mssql_type(value)
                columns.append(f"[{key}] {sql_type}")
        
        # Create table query
        create_query = f"""
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = '{table_name}')
        BEGIN
            CREATE TABLE {table_name} (
                {', '.join(columns)}
            )
        END
        """
        
        conn = get_mssql_connection()
        if conn:
            cursor = conn.cursor()
            cursor.execute(create_query)
            conn.commit()
            conn.close()
            print(f"Created table {table_name}")
            return True
        return False
        
    except Exception as e:
        print(f"Error creating table for {measurement}: {e}")
        return False

def sync_data():
    if not influx_client and not connect_influxdb():
        return
    
    try:
        conn = get_mssql_connection()
        if not conn:
            return
        
        cursor = conn.cursor()
        
        for measurement in MEASUREMENT_LIST:
            table_name = f"{measurement}_tb"
            
            # Get latest timestamp from MSSQL
            cursor.execute(f"SELECT MAX(time) FROM {table_name}")
            last_time = cursor.fetchone()[0]
            
            # Query InfluxDB
            where_clause = f"WHERE time > '{last_time.isoformat()}'" if last_time else ""
            query = f'SELECT * FROM "{measurement}" {where_clause}'
            result = influx_client.query(query)
            
            points = list(result.get_points())
            
            # Get table columns
            cursor.execute(f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table_name}'")
            valid_columns = [row[0] for row in cursor.fetchall()]
            
            for point in points:
                columns = [col for col in point.keys() if col in valid_columns]
                values = [f"'{point[col]}'" if isinstance(point[col], str) else str(point[col]) 
                         for col in columns]
                
                # Check if record exists
                time_value = point['time']
                check_query = f"SELECT COUNT(*) FROM {table_name} WHERE time = '{time_value}'"
                cursor.execute(check_query)
                exists = cursor.fetchone()[0] > 0
                
                if not exists:
                    insert_query = f"""
                    INSERT INTO {table_name} ({','.join(columns)})
                    VALUES ({','.join(values)})
                    """
                    cursor.execute(insert_query)
        
        conn.commit()
        conn.close()
        
    except Exception as e:
        print(f"Error syncing data: {e}")
        if "connection" in str(e).lower():
            connect_influxdb()  # Reconnect if connection issue

def main():
    # Initial connection
    connect_influxdb()
    
    # Create tables for all measurements
    for measurement in MEASUREMENT_LIST:
        create_table_auto(measurement)
    
    # Main loop
    while True:
        sync_data()
        time.sleep(INTERVAL)

if __name__ == "__main__":
    main()