import time
import datetime
import pyodbc
from influxdb import InfluxDBClient
from dotenv import load_dotenv
import os
import logging  # Added for logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Error logger
error_logger = logging.getLogger('error_logger')
error_handler = logging.FileHandler('error.log')
error_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
error_logger.addHandler(error_handler)
error_logger.setLevel(logging.ERROR)

# Success logger
success_logger = logging.getLogger('success_logger')
success_handler = logging.FileHandler('success.log')
success_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
success_logger.addHandler(success_handler)
success_logger.setLevel(logging.INFO)

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

def get_tools_from_mssql():
    try:
        # Connect to MSSQL
        conn = connect_mssql()
        if not conn:
            error_msg = "[get_tools_from_mssql] Failed to connect to MSSQL"
            print(error_msg)
            error_logger.error(error_msg)
            return []

        cursor = conn.cursor()
        
        # Query distinct tools from device_master_tb
        query = "SELECT DISTINCT tools FROM device_master_tb WHERE tools IS NOT NULL"
        cursor.execute(query)
        
        # Get all tools and add 'iot_' prefix
        measurement_list = [f"iot_{row[0]}" for row in cursor.fetchall() if row[0]]
        
        # Close connection
        cursor.close()
        conn.close()
        
        # Return sorted unique measurements
        return sorted(list(set(measurement_list)))
    
    except Exception as e:
        error_msg = f"[get_measurements_from_influxdb] Error fetching measurements from MSSQL: {str(e)}"
        print(error_msg)
        error_logger.error(error_msg)
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
        return "DATETIME2(6)"
    return "NVARCHAR(255)"  # Default type

def create_table_mssql(measurement):
    table_name = f"{measurement}_tb"
    column_info = []
    
    mssql_conn = connect_mssql()
    if not mssql_conn:
        error_msg = f"[create_table_mssql] Failed to connect to MSSQL for creating table {table_name}"
        error_logger.error(error_msg)
        return None
    
    try:
        cursor = mssql_conn.cursor()
        
        cursor.execute(f"""
            SELECT COUNT(*) 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_NAME = '{table_name}'
        """)
        table_exists = cursor.fetchone()[0] > 0
        
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
            if not influx_client:
                error_msg = f"[create_table_mssql] No InfluxDB connection"
                print(error_msg)
                error_logger.error(error_msg)
                return None
                
            query = f'SELECT * FROM "{measurement}" ORDER BY time DESC LIMIT 1'
            result = influx_client.query(query)
            
            if not result:
                error_msg = f"[create_table_mssql] No data found for measurement {measurement}"
                print(error_msg)
                error_logger.error(error_msg)
                return None
                
            points = list(result.get_points())
            if not points:
                error_msg = f"[create_table_mssql] No points found for measurement {measurement}"
                print(error_msg)
                error_logger.error(error_msg)
                return None
                
            latest_point = points[0]
            
            columns = []
            column_info.append(("time", "DATETIME2(6)"))
            columns.append("time DATETIME2(6)")
            
            for key, value in latest_point.items():
                if key != 'time':
                    sql_type = map_influx_to_mssql_type(value)
                    columns.append(f"[{key}] {sql_type}")
                    column_info.append((key, sql_type))
            
            create_table_sql = f"""
                CREATE TABLE {table_name} (
                    {', '.join(columns)}
                )
            """
            cursor.execute(create_table_sql)
            print(f"Created table {table_name} successfully")
        
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
        error_msg = f"[create_table_mssql] Error creating table/type {table_name}: {str(e)}"
        print(error_msg)
        error_logger.error(error_msg)
        return None
    finally:
        if mssql_conn:
            mssql_conn.close()

def fetch_influxdb_data(column_info, measurement, time_exit):
    try:
        now = datetime.datetime.utcnow()
        
        if time_exit:
            start_time = time_exit + datetime.timedelta(microseconds=1)
        else:
            start_time = now - datetime.timedelta(minutes=INTERVAL * 5, seconds=now.second, microseconds=now.microsecond)
        start_time_str = start_time.strftime('%Y-%m-%dT%H:%M:%S.%fZ')

        columns = [col[0] for col in column_info if col[0] != 'time']
        columns_str = ", ".join(columns) or "*"  # ถ้าไม่มีคอลัมน์ให้ใช้ "*"
        query = f'SELECT {columns_str} FROM "{measurement}" WHERE time >= \'{start_time_str}\' ORDER BY time ASC'
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
                    elif dtype == "DATETIME2(6)" and not isinstance(value, datetime.datetime):
                        try:
                            transformed_point[column] = datetime.datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%fZ")
                        except ValueError:
                            transformed_point[column] = datetime.datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ")
                    else:
                        transformed_point[column] = value
                elif column == 'time' and 'time' in point:
                    time_value = point['time']
                    if isinstance(time_value, str):
                        try:
                            # ลองแยกวิเคราะห์แบบมีไมโครวินาที
                            time_value = datetime.datetime.strptime(time_value, "%Y-%m-%dT%H:%M:%S.%fZ")
                        except ValueError:
                            # ถ้าล้มเหลว ให้ใช้รูปแบบที่ไม่มีไมโครวินาที
                            time_value = datetime.datetime.strptime(time_value, "%Y-%m-%dT%H:%M:%SZ")
                    time_value = time_value + datetime.timedelta(hours=7)
                    transformed_point['time'] = time_value

            transformed_data.append(transformed_point)

        return transformed_data
    except Exception as e:
        error_msg = f"[fetch_influxdb_data] Error fetching data from InfluxDB: {str(e)}"
        print(error_msg)
        error_logger.error(error_msg)
        return []

def insert_mssql(data, table_name):
    if not data:
        success_msg = f"[insert_mssql] No data to insert into {table_name}"
        print(success_msg)
        success_logger.info(success_msg)  # Log success to success.log
        return
        
    measurement = table_name.replace('_tb', '')
    tvp_type = f"{measurement}_tvp_type"
    
    try:
        conn = connect_mssql()
        if not conn:
            error_msg = f"[insert_mssql] MSSQL connection failed"
            print(error_msg)
            error_logger.error(error_msg)
            return
            
        cursor = conn.cursor()
        
        columns = list(data[0].keys())
        tvp_data = [tuple(row[col] for col in columns) for row in data]
        
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
        
        sql = f"EXEC usp_Insert_{measurement} @tvp=?"
        cursor.execute(sql, (tvp_data,))
        
        conn.commit()
        success_msg = f"[insert_mssql] Successfully inserted {len(data)} rows into {table_name} using TVP"
        print(success_msg)
        success_logger.info(success_msg)  # Log success to success.log
        
    except Exception as e:
        error_msg = f"[insert_mssql] Error inserting data into MSSQL using TVP: {str(e)}"
        print(error_msg)
        error_logger.error(error_msg)
    finally:
        if conn:
            conn.close()

def get_last_time(table_name):
    try:
        conn = connect_mssql()
        if not conn:
            error_msg = f"[get_last_time] Error connecting to MSSQL for table {table_name}"
            print(error_msg)
            error_logger.error(error_msg)
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
        error_msg = f"[get_last_time] Error fetching latest time from MSSQL for table {table_name}: {str(e)}"
        print(error_msg)
        error_logger.error(error_msg)
        return None
    
    finally:
        if conn:
            conn.close()

def main():
    while True:
        try:
            start_time = time.time()
            if not connect_influxdb():
                time.sleep(1)
                continue
            MEASUREMENT_LIST = get_tools_from_mssql()
            if not MEASUREMENT_LIST:
                print("No matching measurements found")
                time.sleep(INTERVAL*30)
                continue
                
            print(f"Processing measurements: {MEASUREMENT_LIST}")
            for measurement in MEASUREMENT_LIST:
                column_info = create_table_mssql(measurement)
                if column_info is not None:
                    last_time = get_last_time(f"{measurement}_tb")
                    influx_data = fetch_influxdb_data(column_info, measurement, last_time)
                    print("ok")
                    insert_mssql(influx_data, f"{measurement}_tb")
            elapsed_time = time.time() - start_time
            remaining_time = max(0, INTERVAL*60 - elapsed_time)
            time.sleep(remaining_time)
        except Exception as e:
            error_msg = f"[main] Main error: {str(e)}"
            print(error_msg)
            error_logger.error(error_msg)

if __name__ == "__main__":
    main()