# Test_telegraf

CREATE RETENTION POLICY "7_days" ON "test_db" DURATION 7d REPLICATION 1
ALTER RETENTION POLICY "7_days" ON "test_db" DEFAULT

DROP PROCEDURE usp_Insert_test_data;
DROP TYPE dbo.test_data_tvp_type;

DROP PROCEDURE usp_Insert_test_data2;
DROP TYPE dbo.test_data2_tvp_type;

## sent data
topic: iot_sensors/iot_{tools}/{machine_name}

## device_alive table
table_name: device_master_tb
topic: iot_sensors/device_{tools}/{machine_name}
cycle = 30 sec
id || tools || machine || process || location

create device_master_tb
-- ใช้ฐานข้อมูล iot_db
USE iot_db;
-- สร้างตาราง device_master_tb
CREATE TABLE device_master_tb (
    id INT NOT NULL IDENTITY(1,1),
    tools NVARCHAR(50) NULL,
    machine NVARCHAR(50) NULL,
    process NVARCHAR(50) NULL,
    location NVARCHAR(50) NULL,
    PRIMARY KEY (id)
);


-- สร้างตารางชั่วคราวสำหรับเก็บรหัส machine จาก mc_01 ถึง mc_200
WITH MachineNumbers AS (
    -- สร้างลำดับตัวเลข 1 ถึง 200
    SELECT RIGHT('00' + CAST(number AS NVARCHAR(3)), 2) AS machine_num
    FROM master.dbo.spt_values
    WHERE type = 'P' AND number BETWEEN 1 AND 200
)
-- เพิ่มข้อมูลลงใน device_master_tb
INSERT INTO device_master_tb (tools, machine, process, location)
SELECT 'got1', 'mc_' + machine_num, 'A', 'B'
FROM MachineNumbers
UNION ALL
SELECT 'got2', 'mc_' + machine_num, 'A', 'B'
FROM MachineNumbers;

