# Test_telegraf

CREATE RETENTION POLICY "7_days" ON "test_db" DURATION 7d REPLICATION 1
ALTER RETENTION POLICY "7_days" ON "test_db" DEFAULT

DROP PROCEDURE usp_Insert_test_data;
DROP TYPE dbo.test_data_tvp_type;
