import time
import datetime
import pymssql
from influxdb import InfluxDBClient
from datetime import timedelta
import paho.mqtt.client as mqtt
import re
import json
from dotenv import load_dotenv
import os

# โหลดค่าจากไฟล์ .env
load_dotenv()

MEASUREMENT_LIST = ['test_data', 'test_data2', 'mesure_2', 'mesure_3', 'mesure_4']
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
# Updated map_influx_to_mssql_type function
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
        return None  # ถ้าไม่สามารถเชื่อมต่อ MSSQL ให้คืนค่า None
    
    try:
        cursor = mssql_conn.cursor()
        
        # Check if table exists
        cursor.execute(f"""
            SELECT COUNT(*) 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_NAME = '{table_name}'
        """)
        table_exists = cursor.fetchone()[0] > 0
        
        if table_exists:
            print(f"Table {table_name} already exists")
            
            # ดึงข้อมูลคอลัมน์ที่มีอยู่ในตาราง
            cursor.execute(f"""
                SELECT COLUMN_NAME, DATA_TYPE
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME = '{table_name}'
            """)
            columns = cursor.fetchall()
            
            # สร้าง column_info จากข้อมูลที่ดึงมา
            for column in columns:
                column_info.append((column[0], column[1]))
            return column_info  # คืนค่า column_info ถ้าตารางมีอยู่แล้ว
            
        # Get latest data from InfluxDB to determine schema
        if not influx_client:
            print("No InfluxDB connection")
            return None  # ถ้าไม่สามารถเชื่อมต่อ InfluxDB ให้คืนค่า None
            
        query = f'SELECT * FROM "{measurement}" ORDER BY time DESC LIMIT 1'
        result = influx_client.query(query)
        
        if not result:
            print(f"No data found for measurement {measurement}")
            return None
        
        # Get the latest record
        points = list(result.get_points())
        if not points:
            print(f"No points found for measurement {measurement}")
            return None
            
        latest_point = points[0]
        
        # Create table schema
        columns = []
        column_info.append(("time", "DATETIME"))  # เก็บชื่อคอลัมน์ "time" และชนิดข้อมูล "DATETIME"
        columns.append("time DATETIME")  # Add time column first (required for time-series data)
        
        # Add other fields based on the latest data point
        for key, value in latest_point.items():
            if key != 'time':  # Skip time as it's already added
                sql_type = map_influx_to_mssql_type(value)
                columns.append(f"[{key}] {sql_type}")
                column_info.append((key, sql_type))  # เก็บชื่อคอลัมน์และชนิดข้อมูล
        
        # Create table SQL statement
        create_table_sql = f"""
            CREATE TABLE {table_name} (
                {', '.join(columns)}
            )
        """
        
        # Execute table creation
        cursor.execute(create_table_sql)
        mssql_conn.commit()
        print(f"Created table {table_name} successfully with columns: {column_info}")
        
        return column_info  # คืนค่าข้อมูลชื่อคอลัมน์และชนิดข้อมูล
        
    except Exception as e:
        print(f"Error creating table {table_name}: {e}")
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
        # print(f"Query: {query}")
        
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
    try:
        # เชื่อมต่อ MSSQL
        conn = connect_mssql()
        if not conn:
            print("MSSQL connection failed")
            return
        
        cursor = conn.cursor()

        # สร้างชื่อคอลัมน์จาก column_info
        columns = ", ".join([column[0] for column in data[0].items()])
        # สร้างค่า placeholders (สำหรับ values) ที่จะถูกแทรก
        placeholders = ", ".join(["%s"] * len(data[0]))

        # สร้าง query INSERT
        insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        print("2")
        # ดึงข้อมูลที่ต้องการแทรก
        values_to_insert = []
        for row in data:
            values_to_insert.append(tuple(row.values()))  # แปลงแต่ละแถวเป็น tuple ของค่าต่างๆ
        print("3")
        # แทรกข้อมูลแบบ Batch
        cursor.executemany(insert_query, values_to_insert)
        print("4")
        conn.commit()  # คอมมิทการเปลี่ยนแปลง

        print(f"Successfully inserted {len(data)} rows into {table_name}")
    
    except Exception as e:
        print(f"Error inserting data into MSSQL: {e}")
    
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

        latest_time = row[0] - datetime.timedelta(hours=7)  # ลบเวลา 7 ชั่วโมง
        print(f"Latest time fetched from {table_name} (adjusted): {latest_time}")
        return latest_time  # คืนค่า time ล่าสุดที่ปรับแล้ว
        
    except Exception as e:
        print(f"Error fetching latest time from MSSQL for table {table_name}: {e}")
        return None
    
    finally:
        if conn:
            conn.close()


# Modified main function
def main():
    while True:
        try:
            start_time = time.time()
            if not connect_influxdb():
                time.sleep(1)  # Wait before retrying
                continue  
            for measurement in MEASUREMENT_LIST:
                column_info = create_table_mssql(measurement)
                if column_info is not None:  # ใช้ != เพื่อเช็คว่า column_info ไม่ใช่ None
                    time_exit = time_exited(f"{measurement}_tb")
                    influx_data = fetch_influxdb_data(column_info,measurement,time_exit)
                    print("ok")
                    insert_mssql(influx_data,f"{measurement}_tb")
            use_time = time.time() - start_time
            print("use_time: ",use_time)
            time.sleep(INTERVAL*60)  # Wait for next iteration
        except Exception as e:
            print(f"Main error: {e}")
# ==========================
# 🔹 RUN SCRIPT
# ==========================
if __name__ == "__main__":
    main()