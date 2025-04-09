import os
import time
from datetime import datetime, timedelta
import pymssql
from influxdb import InfluxDBClient
from dotenv import load_dotenv
from dateutil import parser
# โหลดค่าจากไฟล์ .env
load_dotenv()

MEASUREMENT_LIST = ['test_data', 'mesure_2', 'mesure_3', 'mesure_4']

# MSSQL CONFIG
MSSQL_CONFIG = {
    'server': os.getenv("MSSQL_SERVER"),
    'user': os.getenv("MSSQL_USER"),
    'password': os.getenv("MSSQL_PASSWORD"),
    'database': os.getenv("MSSQL_DATABASE"),
    'port': int(os.getenv("MSSQL_PORT", 1433)),
    'autocommit': True
}

# INFLUX CONFIG
INFLUX_CONFIG = {
    'host': os.getenv("INFLUXDB_HOST"),
    'port': int(os.getenv("INFLUXDB_PORT", 8086)),
    'database': os.getenv("INFLUXDB_DATABASE")
}


def connect_influx():
    while True:
        try:
            client = InfluxDBClient(**INFLUX_CONFIG)
            client.ping()
            print("✅ Connected to InfluxDB")
            return client
        except Exception as e:
            print("❌ InfluxDB connection failed, retrying in 5s:", e)
            time.sleep(5)


def connect_mssql():
    return pymssql.connect(**MSSQL_CONFIG)


def get_latest_data_point(client, measurement):
    query = f"SELECT * FROM {measurement} ORDER BY time DESC LIMIT 1"
    result = list(client.query(query).get_points())
    return result[0] if result else None


def map_influx_type_to_sql(value):
    if isinstance(value, bool):
        return "BIT"
    elif isinstance(value, int):
        return "INT"
    elif isinstance(value, float):
        return "FLOAT"
    else:
        return "NVARCHAR(MAX)"


def create_table_if_not_exists(mssql_conn, measurement, sample):
    table_name = f"{measurement}_tb"
    cursor = mssql_conn.cursor()
    columns_sql = ', '.join([f"[{k}] {map_influx_type_to_sql(v)}" for k, v in sample.items() if k != 'time'])
    cursor.execute(f"""
        IF NOT EXISTS (
            SELECT * FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_NAME = '{table_name}'
        )
        BEGIN
            CREATE TABLE {table_name} (
                time DATETIME PRIMARY KEY
                {', ' + columns_sql if columns_sql else ''}
            )
        END
    """)
    cursor.close()
    print(f"📦 Table ready: {table_name}")


def get_existing_times(mssql_conn, table_name):
    cursor = mssql_conn.cursor()
    try:
        five_minutes_ago = datetime.utcnow() - timedelta(minutes=5)
        cursor.execute(f"""
            SELECT time FROM {table_name}
            WHERE time >= %s
        """, (five_minutes_ago,))
        return set(row[0].strftime("%Y-%m-%dT%H:%M:%SZ") for row in cursor.fetchall())
    except Exception as e:
        print(f"⚠️ Error fetching existing times for {table_name}:", e)
        return set()

def insert_new_data(mssql_conn, table_name, data):
    keys = list(data.keys())

    # ✅ แปลง 'time' เป็น datetime object ถ้ามี
    if 'time' in data:
        try:
            data['time'] = parser.isoparse(data['time']).replace(tzinfo=None)  # ลบท้าย 'Z' และ timezone
        except Exception as e:
            print(f"⚠️ Failed to parse time: {data['time']} => {e}")
            return

    values = tuple(data[k] for k in keys)
    placeholders = ', '.join(['%s'] * len(keys))
    columns = ', '.join([f"[{k}]" for k in keys])

    cursor = mssql_conn.cursor()
    try:
        cursor.execute(f"""
            INSERT INTO {table_name} ({columns}) VALUES ({placeholders})
        """, values)
    except Exception as e:
        print("⚠️ Insert error:", e)
    cursor.close()



def sync_data(client, mssql_conn, measurement, interval='3m'):
    table_name = f"{measurement}_tb"
    query = f"SELECT * FROM {measurement} WHERE time > now() - {interval}"
    points = list(client.query(query).get_points())

    if not points:
        print(f"ℹ️ No data for {measurement} in last {interval}")
        return

    existing_times = get_existing_times(mssql_conn, table_name)

    inserted = 0
    for point in points:
        time_key = point.get('time')
        if time_key not in existing_times:
            insert_new_data(mssql_conn, table_name, point)
            inserted += 1

    print(f"✅ [{measurement}] Inserted: {inserted} new record(s)")


def main_loop(interval_seconds=60):
    influx = connect_influx()
    mssql = connect_mssql()

    for measurement in MEASUREMENT_LIST:
        try:
            data = get_latest_data_point(influx, measurement)
            if data:
                create_table_if_not_exists(mssql, measurement, data)
            else:
                print(f"⚠️ No sample data for {measurement}, skipping table creation.")
        except Exception as e:
            print(f"❌ Error initializing {measurement}: {e}")

    while True:
        try:
            for measurement in MEASUREMENT_LIST:
                sync_data(influx, mssql, measurement)
        except Exception as e:
            print("❌ Error in main sync loop:", e)
            influx = connect_influx()
        time.sleep(interval_seconds)


if __name__ == "__main__":
    main_loop()
