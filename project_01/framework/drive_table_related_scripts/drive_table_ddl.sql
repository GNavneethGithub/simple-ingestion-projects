CREATE TABLE IF NOT EXISTS drive_table (
    -- Core Pipeline Identity
    who_ran_pipeline VARCHAR(100) DEFAULT NULL,
    pipeline_name VARCHAR(500) DEFAULT NULL,
    pipeline_priority FLOAT DEFAULT NULL,
    dag_run_id VARCHAR(200) DEFAULT NULL,
    pipeline_parallel_thread_id VARCHAR(200) DEFAULT NULL,
    
    -- Data Flow Locations
    source_name VARCHAR(300) DEFAULT NULL,
    stage_name VARCHAR(300) DEFAULT NULL,
    target_name VARCHAR(300) DEFAULT NULL,
    
    -- Data Extraction Parameters
    query_target_day VARCHAR(50) DEFAULT NULL,
    query_window_start_time VARCHAR(50) DEFAULT NULL,
    query_window_end_time VARCHAR(50) DEFAULT NULL,
    time_interval VARCHAR(50) DEFAULT NULL,
    
    -- Record Audit Timestamps
    record_first_inserted_time VARCHAR(50) DEFAULT NULL,
    record_last_update_time VARCHAR(50) DEFAULT NULL,
    
    -- Component Classification
    source_category VARCHAR(300) DEFAULT NULL,
    source_sub_type VARCHAR(300) DEFAULT NULL,
    stage_category VARCHAR(300) DEFAULT NULL,
    stage_sub_type VARCHAR(300) DEFAULT NULL,
    target_category VARCHAR(500) DEFAULT NULL,
    target_sub_type VARCHAR(300) DEFAULT NULL,
    
    -- Hash-based Identifiers
    source_id VARCHAR(100) DEFAULT NULL,
    stage_id VARCHAR(100) DEFAULT NULL,
    target_id VARCHAR(100) DEFAULT NULL,
    pipeline_id VARCHAR(100) DEFAULT NULL,
    
    -- Source to Stage Transfer Process
    src_stg_xfer_enabled BOOLEAN DEFAULT NULL,
    src_stg_xfer_status VARCHAR(50) DEFAULT 'PENDING',
    src_stg_xfer_start_ts VARCHAR(50) DEFAULT NULL,
    src_stg_xfer_end_ts VARCHAR(50) DEFAULT NULL,
    src_stg_xfer_duration VARCHAR(50) DEFAULT NULL,
    src_stg_xfer_exp_duration VARCHAR(50) DEFAULT NULL,
    
    -- Stage to Target Transfer Process
    stg_tgt_xfer_enabled BOOLEAN DEFAULT NULL,
    stg_tgt_xfer_status VARCHAR(50) DEFAULT 'PENDING',
    stg_tgt_xfer_start_ts VARCHAR(50) DEFAULT NULL,
    stg_tgt_xfer_end_ts VARCHAR(50) DEFAULT NULL,
    stg_tgt_xfer_duration VARCHAR(50) DEFAULT NULL,
    stg_tgt_xfer_exp_duration VARCHAR(50) DEFAULT NULL,
    
    -- Source to Stage Audit Process
    src_stg_audit_enabled BOOLEAN DEFAULT NULL,
    src_stg_audit_status VARCHAR(50) DEFAULT 'PENDING',
    src_stg_audit_start_ts VARCHAR(50) DEFAULT NULL,
    src_stg_audit_end_ts VARCHAR(50) DEFAULT NULL,
    src_stg_audit_duration VARCHAR(50) DEFAULT NULL,
    src_stg_audit_exp_duration VARCHAR(50) DEFAULT NULL,
    
    -- Stage to Target Audit Process
    stg_tgt_audit_enabled BOOLEAN DEFAULT NULL,
    stg_tgt_audit_status VARCHAR(50) DEFAULT 'PENDING',
    stg_tgt_audit_start_ts VARCHAR(50) DEFAULT NULL,
    stg_tgt_audit_end_ts VARCHAR(50) DEFAULT NULL,
    stg_tgt_audit_duration VARCHAR(50) DEFAULT NULL,
    stg_tgt_audit_exp_duration VARCHAR(50) DEFAULT NULL,
    
    -- Data Quality & Validation
    source_count BIGINT DEFAULT NULL,
    stage_count BIGINT DEFAULT NULL,
    target_count BIGINT DEFAULT NULL,
    audit_result VARCHAR(50) DEFAULT NULL,
    can_fetch_historical_data BOOLEAN DEFAULT NULL,
    continuity_check_performed BOOLEAN DEFAULT NULL,
    parallelization_enabled BOOLEAN DEFAULT NULL,
    
    -- Pipeline Execution Tracking
    phase_completed VARCHAR(100) DEFAULT NULL,
    pipeline_start_time VARCHAR(50) DEFAULT NULL,
    pipeline_end_time VARCHAR(50) DEFAULT NULL,
    pipeline_duration VARCHAR(50) DEFAULT NULL,
    pipeline_exp_duration VARCHAR(50) DEFAULT NULL,
    retry_attempt_number INTEGER DEFAULT NULL,
    
    -- Alerting & Miscellaneous
    email_alerts_send_to VARCHAR(1000) DEFAULT NULL,
    miscellaneous_data VARIANT DEFAULT NULL
);