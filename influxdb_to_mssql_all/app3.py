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
import pandas as pd
# ==========================
# 🔹 LOAD ENVIRONMENT VARIABLES
# ==========================
load_dotenv()

# InfluxDB Configuration
INFLUXDB_HOST = os.getenv('INFLUXDB_HOST')
INFLUXDB_PORT = int(os.getenv('INFLUXDB_PORT'))
INFLUXDB_DATABASE = os.getenv('INFLUXDB_DATABASE')

# MSSQL Configuration
MSSQL_SERVER = os.getenv('MSSQL_SERVER')
MSSQL_USER = os.getenv('MSSQL_USER')
MSSQL_PASSWORD = os.getenv('MSSQL_PASSWORD')
MSSQL_DATABASE = os.getenv('MSSQL_DATABASE')
MSSQL_PORT = int(os.getenv('MSSQL_PORT'))

# MQTT Configuration
MQTT_BROKER = os.getenv('MQTT_BROKER')
MQTT_PORT = int(os.getenv('MQTT_PORT'))
MQTT_TOPIC_CANNOT_INSERT = os.getenv('MQTT_TOPIC_CANNOT_INSERT')

# Script Settings
INTERVAL = int(os.getenv('INTERVAL'))
DELAY = int(os.getenv('DELAY'))

# 🔹 Measurement-to-Topics Mapping
MEASUREMENT_TOPIC_MAP = {
    'got': ['iot_sensors/got/mc_01', 'iot_sensors/got/mc_02'],
    'atm': ['iot_sensors/atm/mc_01', 'iot_sensors/atm/mc_02'],
    'machine_temp': ['iot_sensors/machine_temp/mc_01', 'iot_sensors/machine_temp/mc_02'],
    'machine_vibration': ['iot_sensors/machine_vibration/mc_01', 'iot_sensors/machine_vibration/mc_02'],
    'test_data': [f'iot_sensors/test_data/mc_{i}' for i in range(1, 201)]
}

# ==========================
# 🔹 CONNECT TO INFLUXDB
# ==========================
def connect_influxdb():
    try:
        client = InfluxDBClient(host=INFLUXDB_HOST, port=INFLUXDB_PORT)
        client.ping()
        client.switch_database(INFLUXDB_DATABASE)
        print("✅ connected to influxdb")
        return client
    except Exception as e:
        print(f"🚨 InfluxDB Connection Error: {e}")
        return None

influx_client = connect_influxdb()
if not influx_client:
    exit(1)

# ==========================
# 🔹 CONNECT TO MSSQL
# ==========================
def connect_mssql(retries=3, delay=1):
    for attempt in range(retries):
        try:
            conn = pymssql.connect(server=MSSQL_SERVER, user=MSSQL_USER, password=MSSQL_PASSWORD, database=MSSQL_DATABASE, port=MSSQL_PORT)
            return conn
        except pymssql.Error as e:
            print(f"⚠️ MSSQL Connection Failed (Attempt {attempt+1}/{retries}): {e}")
            time.sleep(delay)
    print("🚨 MSSQL Connection Failed. Exiting...")
    exit(1)

# ==========================
# 🔹 MQTT SETUP
# ==========================
mqtt_client = mqtt.Client()
mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)


# ==========================
# 🔹 CREATE TABLES BY TOPIC
# ==========================


def create_mssql_tables():
    conn = connect_mssql()
    cursor = conn.cursor()
    table_columns_map = {}

    for measurement in MEASUREMENT_TOPIC_MAP.keys():
        table_name = f"raw_{measurement}"

        # Check if table exists
        cursor.execute(f"SELECT COUNT(*) FROM sysobjects WHERE name='{table_name}' AND xtype='U'")
        if cursor.fetchone()[0] > 0:
            print(f"⚡ Table '{table_name}' already exists.")
            cursor.execute(f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table_name}' AND COLUMN_NAME NOT IN ('time', 'topic')")
            columns = [row[0] for row in cursor.fetchall()]
            table_columns_map[table_name] = columns
            continue

        # Get sample data from any topic in this measurement
        topics = MEASUREMENT_TOPIC_MAP[measurement]
        sample_found = False
        for topic in topics:
            query = f"SELECT * FROM \"{measurement}\" WHERE topic = '{topic}' LIMIT 1"
            result = influx_client.query(query)
            points = list(result.get_points())
            if points:
                sample_found = True
                break

        if not sample_found:
            print(f"⚠️ No sample data for measurement '{measurement}', skipping table creation.")
            continue

        data_keys = [key for key in points[0].keys() if key not in ['time', 'topic', 'host']]
        sample_point = points[0]

        columns_with_type = []
        for key in data_keys:
            value = sample_point.get(key)
            sql_type = infer_sql_type_from_value(value)
            columns_with_type.append(f"[{key}] {sql_type}")

        columns_sql = ", ".join(columns_with_type)

        create_query = f"""
        CREATE TABLE {table_name} (
            time DATETIME PRIMARY KEY,
            topic VARCHAR(255),
            {columns_sql}
        );
        """
        cursor.execute(create_query)
        conn.commit()
        print(f"✅ Table '{table_name}' created with columns: {', '.join(data_keys)}")
        table_columns_map[table_name] = data_keys

    cursor.close()
    conn.close()
    return table_columns_map

def infer_sql_type_from_value(value):
    if isinstance(value, bool):
        return "BIT"
    elif isinstance(value, int):
        return "INT"
    elif isinstance(value, float):
        return "FLOAT"
    elif isinstance(value, str):
        return "NVARCHAR(255)"
    else:
        return "NVARCHAR(MAX)"  # fallback กรณีเป็น type อื่นหรือ complex

# ==========================
# 🔹 SANITIZE TABLE NAME
# ==========================
def sanitize_table_name(topic):
    if topic.startswith("iot_sensors/"):
        topic = topic[len("iot_sensors/"):]
    return f"raw_{re.sub(r'[^a-zA-Z0-9_]', '_', topic)}"

# ==========================
# 🔹 FETCH DATA FROM INFLUXDB
# ==========================
def fetch_influxdb_data():
    now = datetime.datetime.utcnow()
    start_time = now - datetime.timedelta(minutes=INTERVAL * 3, seconds=now.second, microseconds=now.microsecond)
    end_time = start_time + datetime.timedelta(minutes=INTERVAL*3)

    all_data = {}

    for measurement in MEASUREMENT_TOPIC_MAP.keys():
        table_name = f"raw_{measurement}"
        all_data[table_name] = []

        query = f"""
            SELECT * FROM \"{measurement}\"
            WHERE time >= '{start_time.isoformat()}Z' AND time < '{end_time.isoformat()}Z'
        """
        result = influx_client.query(query)
        all_data[table_name].extend(list(result.get_points()))

    return all_data
# ==========================
# 🔹 INSERT INTO MSSQL
# ==========================
def insert_data_to_mssql(data):
    conn = connect_mssql()
    cursor = conn.cursor()

    for table_name, rows in data.items():
        if not rows:
            print(f"⚠️ No data to insert for table: {table_name}")
            continue

        try:
            insert_values = []
            for row in rows:
                timestamp = datetime.datetime.strptime(row['time'], '%Y-%m-%dT%H:%M:%S.%fZ') + timedelta(hours=7)
                topic = row['topic']
                values = {key: row[key] for key in row if key not in ['time', 'topic', 'host']}

                check_query = f"SELECT COUNT(*) FROM {table_name} WHERE time = %s AND topic = %s"
                cursor.execute(check_query, (timestamp, topic))
                if cursor.fetchone()[0] == 0:
                    insert_values.append((timestamp, topic, *values.values()))

            if not insert_values:
                print(f"⚠️ No new data to insert for table: {table_name}")
                continue

            columns = ', '.join(['time', 'topic'] + list(values.keys()))
            placeholders = ', '.join(['%s'] * (len(values) + 2))
            insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

            cursor.executemany(insert_query, insert_values)
            conn.commit()

            for timestamp, topic, *vals in insert_values:
                mqtt_message = {
                    "data_id": values.get("data_id", None),
                    "status": "success",
                    "error": "ok",
                    "timestamp": timestamp.isoformat(),
                    "table_name": table_name
                }
                mqtt_client.publish(MQTT_TOPIC_CANNOT_INSERT, json.dumps(mqtt_message))
            print(f"✅ Inserted {len(insert_values)} rows into: {table_name}")

        except Exception as e:
            conn.rollback()
            mqtt_message = {
                "data_id": rows[0].get("data_id", None) if rows else None,
                "status": "fail",
                "error": str(e),
                "timestamp": datetime.datetime.utcnow().isoformat(),
                "table_name": table_name
            }
            mqtt_client.publish(MQTT_TOPIC_CANNOT_INSERT, json.dumps(mqtt_message))
            print(f"⚠️ Failed to insert into: {table_name} | Error: {e}")

    cursor.close()
    conn.close()

def filter_data_by_table_schema_with_types(all_data):
    conn = connect_mssql()
    cursor = conn.cursor()
    filtered_data = {}

    for table_name, rows in all_data.items():
        if not rows:
            print(f"⚠️ No rows in table: {table_name}")
            filtered_data[table_name] = []
            continue

        cursor.execute(f"""
            SELECT COLUMN_NAME, DATA_TYPE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = '{table_name}'
              AND COLUMN_NAME NOT IN ('time', 'topic')
        """)
        result = cursor.fetchall()
        column_types = {col[0]: col[1].lower() for col in result}

        if not column_types:
            print(f"⚠️ No columns found in schema for table: {table_name}")
            filtered_data[table_name] = []
            continue

        df = pd.DataFrame(rows)
        filtered_df = df[['time', 'topic']].copy()

        for col_name, data_type in column_types.items():
            if col_name in df.columns:
                try:
                    if data_type in ['float', 'real']:
                        filtered_df[col_name] = pd.to_numeric(df[col_name], errors='coerce').astype(float)
                    elif data_type in ['int', 'bigint', 'smallint', 'tinyint']:
                        filtered_df[col_name] = pd.to_numeric(df[col_name], errors='coerce').astype('Int64')
                    elif data_type in ['bit']:
                        filtered_df[col_name] = df[col_name].astype(bool)
                    elif data_type in ['nvarchar', 'varchar', 'text']:
                        filtered_df[col_name] = df[col_name].astype(str)
                    elif data_type in ['datetime', 'smalldatetime']:
                        filtered_df[col_name] = df[col_name].astype(str)
                    else:
                        filtered_df[col_name] = df[col_name].astype(str)
                except Exception as e:
                    print(f"⚠️ Skipping column '{col_name}' due to: {e}")
                    filtered_df[col_name] = None

        filtered_data[table_name] = filtered_df.to_dict(orient='records')
        print(f"✅ Filtered data for table: {table_name} completed (rows: {len(filtered_data[table_name])})")

    cursor.close()
    conn.close()
    return filtered_data
def main():
    # 🔹 สร้างตารางก่อน และรับ mapping ของ column ที่สร้าง
    
    while True:
        time.sleep(DELAY)
        try:
            start = time.time()
            create_mssql_tables()
            # 🔹 ดึงข้อมูลจาก InfluxDB
            influx_data = fetch_influxdb_data()
            # 🔹 กรองเฉพาะคีย์ที่ตรงกับ column ที่สร้างไว้ใน MSSQL
            filtered_data = filter_data_by_table_schema_with_types(influx_data)
            print("filtered_data: clear")
            # 🔹 Insert ข้อมูล
            if filtered_data:
                
                insert_data_to_mssql(filtered_data)
                 
            else:
                print("❌ No matching data to insert!")

        except Exception as e:
            mqtt_message = {
                "data_id": -1,
                "status": "fail",
                "error": str(e),
                "timestamp": datetime.datetime.utcnow().isoformat(),
                "table_name": "-1"
            }
            mqtt_client.publish(MQTT_TOPIC_CANNOT_INSERT, json.dumps(mqtt_message))
            print(f"🚨 Error: {e}")
        use_time = time.time()-start
        print(use_time)
        if use_time < INTERVAL * 60 - DELAY:
            time.sleep(INTERVAL * 60 - DELAY)


# ==========================
# 🔹 RUN SCRIPT
# ==========================
if __name__ == "__main__":
    main()
