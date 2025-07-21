# Drive Table Column Meaning and Names

## Overview
The **drive_table** serves as a comprehensive monitoring, auditing, and alerting system for data pipeline operations. Each record represents a complete pipeline execution cycle, tracking data movement from source → stage → target with detailed process monitoring and audit capabilities.

## Table Purpose
- **Monitoring**: Track pipeline execution status and performance
- **Auditing**: Record data quality checks and volume validation
- **Alerting**: Provide data for automated alert systems based on failures, delays, or anomalies

---

## Column Definitions

### Core Pipeline Identity
| Column Name | Data Type | Description |
|-------------|-----------|-------------|
| `who_ran_pipeline` | VARCHAR | Tracks execution method for manual intervention analysis. Helps determine how many times manual process vs airflow completed the process |
| `pipeline_name` | VARCHAR | User-defined pipeline identifier. Can be simple (elasticsearch, aws s3, snowflake) or complex (xyz123asbugdf;fj) - completely flexible naming |
| `pipeline_priority` | FLOAT | Pipeline execution priority where lower number = higher priority. Used for urgent reprocessing (e.g., managers assign 1.3 for critical monthly data refill). Pipelines execute in ascending priority order |
| `dag_run_id` | VARCHAR | Links to the specific Airflow DAG execution that ran this pipeline |
| `pipeline_parallel_thread_id` | VARCHAR | Identifies individual parallel executions within the same DAG run. Helps pinpoint specific thread failures during parallel processing |

### Data Flow Locations
| Column Name | Data Type | Description |
|-------------|-----------|-------------|
| `source_name` | VARCHAR | Name of source data location (e.g., elasticsearch) |
| `stage_name` | VARCHAR | Name of staging data location (e.g., aws s3) |
| `target_name` | VARCHAR | Name of target data location (e.g., snowflake) |

### Data Extraction Parameters
| Column Name | Data Type | Description |
|-------------|-----------|-------------|
| `query_target_day` | VARCHAR | Target date for data processing - used in filters for incremental data collection |
| `query_window_start_time` | VARCHAR | Start time filter applied to source data for incremental collection (ISO string format) |
| `query_window_end_time` | VARCHAR | End time filter applied to source data for incremental collection (ISO string format) |
| `time_interval` | VARCHAR | Calculated duration (query_window_end_time - query_window_start_time). Format: "1d", "12h", "30m", "40s" |

### Record Audit Timestamps
| Column Name | Data Type | Description |
|-------------|-----------|-------------|
| `record_first_inserted_time` | VARCHAR | When this monitoring record was first created in drive table - helps detect data manipulation (ISO string format) |
| `record_last_update_time` | VARCHAR | When this monitoring record was last modified - helps detect data manipulation (ISO string format) |

### Component Classification
| Column Name | Data Type | Description |
|-------------|-----------|-------------|
| `source_category` | VARCHAR | Source granular identification (e.g., cluster name like "clusteriq") |
| `source_sub_type` | VARCHAR | Source specific data subset (e.g., "pending jobs") |
| `stage_category` | VARCHAR | Stage granular identification (e.g., bucket name like "xyz bucket name") |
| `stage_sub_type` | VARCHAR | Stage path/prefix information (e.g., "prefix path") |
| `target_category` | VARCHAR | Target granular identification (e.g., "database_name.schema_name.table_name") |
| `target_sub_type` | VARCHAR | Target file information (e.g., "filename") |

### Hash-based Identifiers
| Column Name | Data Type | Description |
|-------------|-----------|-------------|
| `source_id` | VARCHAR | Hash of (source_name + category + sub_type + query_window timestamps) - tracks data lineage |
| `stage_id` | VARCHAR | Hash of (stage_name + category + sub_type + query_window timestamps) - tracks data lineage |
| `target_id` | VARCHAR | Hash of (target_name + category + sub_type + query_window timestamps) - tracks data lineage |
| `pipeline_id` | VARCHAR | Hash of (source_id + stage_id + target_id) - detects routing errors |

### Source to Stage Transfer Process
| Column Name | Data Type | Description |
|-------------|-----------|-------------|
| `src_stg_xfer_enabled` | BOOLEAN | Flag to skip process when data already transferred by other teams |
| `src_stg_xfer_status` | VARCHAR | Execution state: "in_process", "completed", or "pending" |
| `src_stg_xfer_start_ts` | VARCHAR | Actual process start time (ISO string format) |
| `src_stg_xfer_end_ts` | VARCHAR | Actual process end time (ISO string format) |
| `src_stg_xfer_duration` | VARCHAR | Time taken in readable format (1h, 30m, 45s, 1d) |
| `src_stg_xfer_exp_duration` | VARCHAR | Expected/average duration for comparison and alerting |

### Stage to Target Transfer Process
| Column Name | Data Type | Description |
|-------------|-----------|-------------|
| `stg_tgt_xfer_enabled` | BOOLEAN | Flag to skip process when data already transferred by other teams |
| `stg_tgt_xfer_status` | VARCHAR | Execution state: "in_process", "completed", or "pending" |
| `stg_tgt_xfer_start_ts` | VARCHAR | Actual process start time (ISO string format) |
| `stg_tgt_xfer_end_ts` | VARCHAR | Actual process end time (ISO string format) |
| `stg_tgt_xfer_duration` | VARCHAR | Time taken in readable format (1h, 30m, 45s, 1d) |
| `stg_tgt_xfer_exp_duration` | VARCHAR | Expected/average duration for comparison and alerting |

### Source to Stage Audit Process
| Column Name | Data Type | Description |
|-------------|-----------|-------------|
| `src_stg_audit_enabled` | BOOLEAN | Flag to skip process when audit already performed by other teams |
| `src_stg_audit_status` | VARCHAR | Execution state: "in_process", "completed", or "pending" |
| `src_stg_audit_start_ts` | VARCHAR | Actual audit start time (ISO string format) |
| `src_stg_audit_end_ts` | VARCHAR | Actual audit end time (ISO string format) |
| `src_stg_audit_duration` | VARCHAR | Time taken in readable format (1h, 30m, 45s, 1d) |
| `src_stg_audit_exp_duration` | VARCHAR | Expected/average duration for comparison and alerting |

### Stage to Target Audit Process
| Column Name | Data Type | Description |
|-------------|-----------|-------------|
| `stg_tgt_audit_enabled` | BOOLEAN | Flag to skip process when audit already performed by other teams |
| `stg_tgt_audit_status` | VARCHAR | Execution state: "in_process", "completed", or "pending" |
| `stg_tgt_audit_start_ts` | VARCHAR | Actual audit start time (ISO string format) |
| `stg_tgt_audit_end_ts` | VARCHAR | Actual audit end time (ISO string format) |
| `stg_tgt_audit_duration` | VARCHAR | Time taken in readable format (1h, 30m, 45s, 1d) |
| `stg_tgt_audit_exp_duration` | VARCHAR | Expected/average duration for comparison and alerting |

### Data Quality & Validation
| Column Name | Data Type | Description |
|-------------|-----------|-------------|
| `source_count` | BIGINT | Record count at source stage - used for data integrity validation |
| `avg_source_count` | BIGINT | Expected average source count to detect unexpected huge volume spikes |
| `stage_count` | BIGINT | Record count at staging (may be skipped if counting takes too long) |
| `target_count` | BIGINT | Record count at target stage - compared with source for validation |
| `audit_result` | VARCHAR | Data integrity status: "matched" or "mismatched" based on count alignment |
| `can_fetch_historical_data` | BOOLEAN | **NO**: Data exists only 1 day, must fix corrupt data immediately, no historical extraction possible. **YES**: Data persists, historical extraction allowed |
| `continuity_check_performed` | BOOLEAN | Validates that query time windows are continuous with no gaps (no missing hours/days) |
| `parallelization_enabled` | BOOLEAN | Flag indicating whether parallel execution is allowed for this pipeline run |

### Pipeline Execution Tracking
| Column Name | Data Type | Description |
|-------------|-----------|-------------|
| `phase_completed` | VARCHAR | Pipeline progress status: "source to stage complete", "stage to target complete", "audit completed" |
| `pipeline_start_time` | VARCHAR | Overall pipeline execution start time (ISO string format) |
| `pipeline_end_time` | VARCHAR | Overall pipeline execution end time (ISO string format) |
| `pipeline_duration` | VARCHAR | Overall pipeline execution time in readable format (1h, 30m, 45s, 1d) |
| `pipeline_exp_duration` | VARCHAR | Expected overall pipeline duration for comparison and alerting |
| `retry_attempt_number` | INTEGER | Number of times this pipeline has been executed due to failures (connection errors, issues) |

### Alerting & Miscellaneous
| Column Name | Data Type | Description |
|-------------|-----------|-------------|
| `email_alerts_send_to` | VARCHAR | List of email addresses that receive notifications when pipeline failures occur |
| `miscellaneous_data` | VARIANT | Flexible field for additional information: error messages, user notes for managers, or any other data not covered by standard columns |

---

## Usage Guidelines for Consuming Teams

### For Monitoring Dashboards
- Use `*_status` columns for real-time pipeline health visualization
- Compare `*_duration` vs `*_exp_duration` for performance tracking and bottleneck identification
- Compare `source_count` vs `avg_source_count` to identify volume anomalies and unexpected data spikes
- Sort pipelines by `pipeline_priority` ASC for critical-first views
- Track `retry_attempt_number` for stability metrics
- Use `pipeline_parallel_thread_id` to monitor parallel execution performance

### For Alerting Systems
- Trigger alerts on status = 'FAILED' or extended 'in_process' states
- Alert when actual duration significantly exceeds expected duration
- Alert when `source_count` significantly exceeds `avg_source_count` (e.g., >2x average) for volume spike detection
- Escalate alerts faster for lower priority numbers (higher priority)
- Monitor count discrepancies: source_count ≠ target_count indicates data corruption
- Use `email_alerts_send_to` for notification routing

### For Data Integrity & Audit
- Use `audit_result` for compliance reporting and data quality assessment  
- Verify data integrity through source/target count comparisons
- Track `continuity_check_performed` for data completeness validation
- Monitor `can_fetch_historical_data` = NO pipelines for immediate attention on failures

### For Performance Analysis & Troubleshooting
- Use `dag_run_id` to cross-reference with Airflow logs
- Analyze execution patterns using timing columns across different parallel threads
- Track manual vs automated execution ratios using `who_ran_pipeline`
- Use hash-based IDs to identify data lineage issues and routing errors

---

## Critical Data Handling Rules

1. **Data Corruption Response**: When `audit_result` = "mismatched", trigger cleanup and reprocessing
2. **Historical Data Constraints**: When `can_fetch_historical_data` = NO, do not delete corrupt data (no recovery possible)
3. **Priority-Based Processing**: Execute pipelines in ascending order of `pipeline_priority` values
4. **Process Skipping**: When `*_enabled` = FALSE, skip that process step entirely
5. **Continuity Validation**: Ensure no gaps in time window coverage through `continuity_check_performed`
6. **Volume Anomaly Detection**: Monitor `source_count` vs `avg_source_count` for unexpected data volume changes

**Key Updates Made:**
- Added `avg_source_count` column in Data Quality & Validation section
- Updated all timestamp fields to show VARCHAR with ISO string format
- Added `pipeline_duration` and `pipeline_exp_duration` columns
- Updated data types to match DDL (JSON → VARIANT)
- Enhanced usage guidelines for volume anomaly detection
- Added volume anomaly detection to Critical Data Handling Rules

