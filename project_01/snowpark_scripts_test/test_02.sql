CREATE OR REPLACE PROCEDURE sync_table_a_to_b()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    target_date DATE := CURRENT_DATE() - 1;
    existing_count INTEGER;
    inserted_count INTEGER;
BEGIN
    -- Check if already processed
    SELECT COUNT(*) INTO existing_count
    FROM table_b 
    WHERE DATE(some_timestamp_column) = target_date;  -- need timestamp column in table_b
    
    IF (existing_count > 0) THEN
        RETURN 'SKIPPED: Data for ' || target_date || ' already exists';
    END IF;
    
    -- Insert new records
    INSERT INTO table_b (col1, col2, col3, ...)
    SELECT col1, col2, col3, ...
    FROM table_a 
    WHERE DATE(last_updated) = target_date
      AND pipeline_status = 'completed';
    
    GET DIAGNOSTICS inserted_count = row_count;
    
    RETURN 'SUCCESS: Inserted ' || inserted_count || ' records for ' || target_date;
END;
$$;

CREATE OR REPLACE TASK daily_sync_task
WAREHOUSE = 'YOUR_WAREHOUSE'
SCHEDULE = 'USING CRON 0 6 * * * UTC'  -- 6 AM daily
AS
CALL sync_table_a_to_b();

-- Start the task
ALTER TASK daily_sync_task RESUME;




