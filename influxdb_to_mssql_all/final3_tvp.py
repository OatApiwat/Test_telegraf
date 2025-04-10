import time
import datetime
import pyodbc
from influxdb import InfluxDBClient
from datetime import timedelta
import paho.mqtt.client as mqtt
import re
import json
from dotenv import load_dotenv
import os

# โหลดค่าจากไฟล์ .env
load_dotenv()

# MEASUREMENT_LIST = ['test_data', 'test_data2', 'mesure_2', 'mesure_3', 'mesure_4']
MEASUREMENT_PREFIXES = ['test#', 'mesure#']
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
        print(f"InfluxDB connection failed: {e}")
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
                # print(f"Connected to MSSQL using driver: {driver}")
                return conn
            except pyodbc.Error as e:
                # print(f"Failed with driver {driver}: {e}")
                continue
        
        raise Exception("No suitable ODBC driver found")
    
    except Exception as e:
        print(f"MSSQL connection failed: {e}")
        return None

def get_measurements_from_influxdb(prefixes=['test#', 'mesure#']):
    """Get all measurements from InfluxDB matching the given prefixes"""
    global influx_client
    try:
        if not influx_client:
            if not connect_influxdb():
                return []
        
        # Query all measurements
        result = influx_client.query('SHOW MEASUREMENTS')
        all_measurements = [m['name'] for m in result.get_points()]
        
        # Filter measurements based on prefixes
        measurement_list = []
        for prefix in prefixes:
            # Remove the # symbol and use as prefix for filtering
            prefix_base = prefix.rstrip('#')
            matching_measurements = [m for m in all_measurements if m.startswith(prefix_base)]
            measurement_list.extend(matching_measurements)
            
        return sorted(list(set(measurement_list)))  # Remove duplicates and sort
    
    except Exception as e:
        print(f"Error fetching measurements from InfluxDB: {e}")
        return []

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
    elif isinstance(influx_type, datetime.datetime):
        return "DATETIME"
    return "NVARCHAR(255)"  # Default type

def create_table_mssql(measurement):
    table_name = f"{measurement}_tb"
    column_info = []  # ตัวแปรเพื่อเก็บข้อมูลชื่อคอลัมน์และชนิดข้อมูล
    
    # Connect to MSSQL
    mssql_conn = connect_mssql()
    if not mssql_conn:
        return None
    
    try:
        cursor = mssql_conn.cursor()
        
        # Check if table exists
        cursor.execute(f"""
            SELECT COUNT(*) 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_NAME = '{table_name}'
        """)
        table_exists = cursor.fetchone()[0] > 0
        
        # Check if TVP type exists
        cursor.execute(f"""
            SELECT COUNT(*) 
            FROM sys.table_types 
            WHERE name = '{measurement}_tvp_type'
        """)
        tvp_exists = cursor.fetchone()[0] > 0
        
        if table_exists:
            print(f"Table {table_name} already exists")
            cursor.execute(f"""
                SELECT COLUMN_NAME, DATA_TYPE
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME = '{table_name}'
            """)
            columns = cursor.fetchall()
            for column in columns:
                column_info.append((column[0], column[1]))
        else:
            # Get latest data from InfluxDB to determine schema
            if not influx_client:
                print("No InfluxDB connection")
                return None
                
            query = f'SELECT * FROM "{measurement}" ORDER BY time DESC LIMIT 1'
            result = influx_client.query(query)
            
            if not result:
                print(f"No data found for measurement {measurement}")
                return None
                
            points = list(result.get_points())
            if not points:
                print(f"No points found for measurement {measurement}")
                return None
                
            latest_point = points[0]
            
            # Create table schema
            columns = []
            column_info.append(("time", "DATETIME"))
            columns.append("time DATETIME")
            
            for key, value in latest_point.items():
                if key != 'time':
                    sql_type = map_influx_to_mssql_type(value)
                    columns.append(f"[{key}] {sql_type}")
                    column_info.append((key, sql_type))
            
            # Create table
            create_table_sql = f"""
                CREATE TABLE {table_name} (
                    {', '.join(columns)}
                )
            """
            cursor.execute(create_table_sql)
            print(f"Created table {table_name} successfully")
        
        # Create TVP type if it doesn't exist
        if not tvp_exists:
            tvp_columns = [f"{col[0]} {col[1]}" for col in column_info]
            create_tvp_sql = f"""
                CREATE TYPE {measurement}_tvp_type AS TABLE (
                    {', '.join(tvp_columns)}
                )
            """
            cursor.execute(create_tvp_sql)
            print(f"Created TVP type {measurement}_tvp_type")
        
        mssql_conn.commit()
        return column_info
        
    except Exception as e:
        print(f"Error creating table/type {table_name}: {e}")
        return None
    finally:
        if mssql_conn:
            mssql_conn.close()

def fetch_influxdb_data(column_info, measurement, time_exit):
    try:
        now = datetime.datetime.utcnow()
        
        if time_exit:
            start_time = time_exit
        else:
            start_time = now - datetime.timedelta(minutes=INTERVAL * 5, seconds=now.second, microseconds=now.microsecond)
        
        end_time = now - datetime.timedelta(seconds=now.second, microseconds=now.microsecond)

        start_time_str = start_time.strftime('%Y-%m-%dT%H:%M:%SZ')
        end_time_str = end_time.strftime('%Y-%m-%dT%H:%M:%SZ')

        columns = [col[0] for col in column_info if col[0] != 'time']
        columns_str = ", ".join(columns)

        query = f'SELECT {columns_str} FROM "{measurement}" WHERE time > \'{start_time_str}\' AND time <= \'{end_time_str}\''
        
        result = influx_client.query(query)
        
        if not result:
            print(f"No data found for measurement {measurement}")
            return []

        points = list(result.get_points(measurement=measurement))
        
        transformed_data = []
        for point in points:
            transformed_point = {}
            for column, dtype in column_info:
                if column != 'time' and column in point:
                    value = point[column]
                    if dtype == "INT" and not isinstance(value, int):
                        transformed_point[column] = int(value)
                    elif dtype == "FLOAT" and not isinstance(value, float):
                        transformed_point[column] = float(value)
                    elif dtype == "NVARCHAR(255)" and not isinstance(value, str):
                        transformed_point[column] = str(value)
                    elif dtype == "BIT" and not isinstance(value, bool):
                        transformed_point[column] = bool(value)
                    elif dtype == "DATETIME" and not isinstance(value, datetime.datetime):
                        transformed_point[column] = datetime.datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ")
                    else:
                        transformed_point[column] = value
                elif column == 'time' and 'time' in point:
                    time_value = point['time']
                    if isinstance(time_value, str):
                        time_value = datetime.datetime.strptime(time_value, "%Y-%m-%dT%H:%M:%S.%fZ")
                    time_value = time_value + datetime.timedelta(hours=7)
                    time_value = time_value.replace(microsecond=(time_value.microsecond // 1000) * 1000)
                    transformed_point['time'] = time_value
            transformed_data.append(transformed_point)

        return transformed_data
    except Exception as e:
        print(f"Error fetching data from InfluxDB: {e}")
        return []

def insert_mssql(data, table_name):
    if not data:
        print(f"No data to insert into {table_name}")
        return
        
    measurement = table_name.replace('_tb', '')
    tvp_type = f"{measurement}_tvp_type"
    
    try:
        conn = connect_mssql()
        if not conn:
            print("MSSQL connection failed")
            return
            
        cursor = conn.cursor()
        
        # Prepare TVP data
        columns = list(data[0].keys())
        tvp_data = [tuple(row[col] for col in columns) for row in data]
        
        # Create stored procedure if not exists
        cursor.execute(f"""
            IF NOT EXISTS (SELECT * FROM sys.procedures WHERE name = 'usp_Insert_{measurement}')
            BEGIN
                EXECUTE sp_executesql N'
                    CREATE PROCEDURE usp_Insert_{measurement}
                        @tvp {tvp_type} READONLY
                    AS
                    BEGIN
                        INSERT INTO {table_name} ({', '.join(columns)})
                        SELECT {', '.join(columns)}
                        FROM @tvp
                    END
                '
            END
        """)
        
        # Execute insert using TVP
        sql = f"EXEC usp_Insert_{measurement} @tvp=?"
        cursor.execute(sql, (tvp_data,))
        
        conn.commit()
        print(f"Successfully inserted {len(data)} rows into {table_name} using TVP")
        
    except Exception as e:
        print(f"Error inserting data into MSSQL using TVP: {e}")
    finally:
        if conn:
            conn.close()

def time_exited(table_name):
    try:
        conn = connect_mssql()
        if not conn:
            print(f"Error connecting to MSSQL for table {table_name}")
            return None

        cursor = conn.cursor()
        
        query = f"""
            SELECT TOP 1 time 
            FROM {table_name} 
            ORDER BY time DESC
        """
        cursor.execute(query)
        row = cursor.fetchone()
        
        if not row:
            print(f"No data found in {table_name}")
            return None

        latest_time = row[0] - datetime.timedelta(hours=7)
        print(f"Latest time fetched from {table_name} (adjusted): {latest_time}")
        return latest_time
        
    except Exception as e:
        print(f"Error fetching latest time from MSSQL for table {table_name}: {e}")
        return None
    
    finally:
        if conn:
            conn.close()

def main():
    while True:
        try:
            if not connect_influxdb():
                time.sleep(1)
                continue
            MEASUREMENT_LIST = get_measurements_from_influxdb(MEASUREMENT_PREFIXES)
            if not MEASUREMENT_LIST:
                print("No matching measurements found")
                time.sleep(INTERVAL*30)
                continue
                
            print(f"Processing measurements: {MEASUREMENT_LIST}")
            for measurement in MEASUREMENT_LIST:
                column_info = create_table_mssql(measurement)
                if column_info is not None:
                    time_exit = time_exited(f"{measurement}_tb")
                    influx_data = fetch_influxdb_data(column_info, measurement, time_exit)
                    print("ok")
                    insert_mssql(influx_data, f"{measurement}_tb")
            
            time.sleep(INTERVAL*60)
        except Exception as e:
            print(f"Main error: {e}")

if __name__ == "__main__":
    main()