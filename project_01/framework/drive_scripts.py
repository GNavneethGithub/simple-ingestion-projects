import re
import pytz
import json
from contextlib import contextmanager
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional

import snowflake.connector
from snowflake.connector import DictCursor

from custom_logger import CustomLogger


def validate_sf_config(sf_config: Dict[str, Any], logger: CustomLogger) -> None:
    """
    Validate Snowflake configuration has all required keys.
    The logger instance is passed as an argument.
    """
    # Define a consistent keyword for all logs within this function
    KEYWORD_SF_CONFIG_VALIDATION = "SF_CONFIG_VALIDATION"

    # Log the start of the validation process using the defined keyword.
    logger.debug(
        message="Starting Snowflake configuration validation.",
        keyword=KEYWORD_SF_CONFIG_VALIDATION,
        other_details={"input_config_type": type(sf_config).__name__, "config_keys": list(sf_config.keys())}
    )

    required_keys = ['account', 'user', 'password', 'warehouse', 'database', 'schema', 'table']
    missing_keys = [key for key in required_keys if key not in sf_config]
    
    # This print statement is for quick, interactive debugging.
    # It will not be part of the final, structured log output.
    print(f"DEBUG: missing_keys found: {missing_keys}")
    
    if missing_keys:
        error_msg = f"Missing required sf_config keys: {missing_keys}"
        
        # Log the failure as an error, using the keyword.
        logger.error(
            message=error_msg,
            keyword=KEYWORD_SF_CONFIG_VALIDATION,
            other_details={"validation_status": "FAILED", "missing_keys": missing_keys}
        )
        raise ValueError(error_msg)

    # Log success with the keyword.
    logger.info(
        message="Snowflake configuration validation passed successfully.",
        keyword=KEYWORD_SF_CONFIG_VALIDATION,
        other_details={"validation_status": "SUCCESS", "validated_keys": required_keys}
    )


def validate_pipeline_id(pipeline_id: Optional[str]) -> str:
    """Validate PIPELINE_ID is not None, empty, or whitespace."""
    print(f"DEBUG: validate_pipeline_id() called")
    print(f"DEBUG: pipeline_id: {pipeline_id}")
    
    if pipeline_id is None:
        error_msg = "PIPELINE_ID cannot be None, empty, or whitespace"
        print(f"DEBUG: {error_msg}")
        raise ValueError(error_msg)
    
    if not isinstance(pipeline_id, str):
        error_msg = f"PIPELINE_ID must be a string, got: {type(pipeline_id)}"
        print(f"DEBUG: {error_msg}")
        raise ValueError(error_msg)
    
    if not pipeline_id.strip():
        error_msg = f"PIPELINE_ID cannot be empty or whitespace, length: {len(pipeline_id)}"
        print(f"DEBUG: {error_msg}")
        raise ValueError(error_msg)
    
    cleaned_id = pipeline_id.strip()
    print(f"DEBUG: cleaned_id: {cleaned_id} | datatype: {type(cleaned_id)}")
    return cleaned_id


def validate_config_structure(config: Dict[str, Any]) -> None:
    """Validate main config has required structure."""
    print(f"DEBUG: validate_config_structure() called")
    #  print(f"DEBUG: config: {type(config)}")
    
    if "sf_drive_config" not in config:
        print(f"DEBUG: config_keys: {list(config.keys())} | datatype: {type(config.keys())}")
        error_msg = "Config must contain 'sf_drive_config' key"
        print(f"DEBUG: {error_msg}")
        raise ValueError(error_msg)
    
    required_pipeline_keys = ['PIPELINE_NAME', 'SOURCE_NAME', 'SOURCE_CATEGORY', 'SOURCE_SUB_TYPE']
    missing_keys = [key for key in required_pipeline_keys if key not in config]
    
    print(f"DEBUG: missing_pipeline_keys: {missing_keys} | datatype: {type(missing_keys)}")
    
    if missing_keys:
        available_keys = [k for k in config.keys() if k != 'password']
        error_msg = f"Missing required config keys: {missing_keys}"
        print(f"DEBUG: available_config_keys: {available_keys} | datatype: {type(available_keys)}")
        print(f"DEBUG: {error_msg}")
        raise ValueError(error_msg)
    
    print(f"DEBUG: Config structure validation passed")



@contextmanager
def get_snowflake_connection(sf_config: Dict[str, Any], logger: CustomLogger):
    """
    Context manager for Snowflake connections with automatic cleanup.
    The logger instance is passed as an argument.
    """
    KEYWORD_SF_CONNECTION = "SF_CONNECTION"
    conn = None

    try:
        # Log the attempt to establish a connection
        logger.info(
            message="Attempting to establish Snowflake connection.",
            keyword=KEYWORD_SF_CONNECTION,
            other_details={
                "account": sf_config.get('account'),
                "database": sf_config.get('database'),
                "schema": sf_config.get('schema')
            }
        )

        conn = snowflake.connector.connect(
            account=sf_config['account'],
            user=sf_config['user'],
            password=sf_config['password'],
            warehouse=sf_config['warehouse'],
            database=sf_config['database'],
            schema=sf_config['schema']
        )
        
        # Log successful connection
        logger.info(
            message=f"Snowflake connection established successfully to {sf_config['database']}.{sf_config['schema']}",
            keyword=KEYWORD_SF_CONNECTION,
            other_details={"status": "SUCCESS"}
        )
        
        yield conn
        
    except Exception as e:
        # Prepare a safe configuration dictionary to log, redacting sensitive information
        safe_config = {k: v for k, v in sf_config.items() if k != 'password'}
        safe_config['password'] = '[REDACTED]'
        
        # Log the connection failure with comprehensive details, including the full config for debugging
        logger.error(
            message=f"Failed to establish Snowflake connection: {e}",
            keyword=KEYWORD_SF_CONNECTION,
            other_details={
                "status": "FAILED",
                "exception_type": type(e).__name__,
                "exception_message": str(e),
                "provided_config": safe_config
            }
        )
        # Add a print statement for quick, non-formatted debugging if needed
        print(f"DEBUG: Connection failed - exception: {str(e)}")
        raise
        
    finally:
        if conn:
            try:
                conn.close()
                # Log successful connection closure
                logger.info(
                    message="Snowflake connection closed successfully.",
                    keyword=KEYWORD_SF_CONNECTION,
                    other_details={"status": "CLOSED"}
                )
            except Exception as e:
                # Log any errors that occur during connection closure
                logger.error(
                    message=f"Error while closing Snowflake connection: {e}",
                    keyword=KEYWORD_SF_CONNECTION,
                    other_details={
                        "status": "CLOSE_FAILED",
                        "exception_type": type(e).__name__,
                        "exception_message": str(e)
                    }
                )



def find_in_process_records(config: Dict[str, Any], logger: CustomLogger) -> List[Dict[str, Any]]:
    """Finds all records with in_process status from drive table.
       whose 
       CONTINUITY_CHECK_PERFORMED = 'YES' 
       and
       CAN_FETCH_HISTORICAL_DATA = 'YES' 
       are must
      """
    print(f"DEBUG: find_in_process_records() called")
    #  print(f"DEBUG: config: {type(config)}")
    
    validate_config_structure(config)
    
    sf_config = config["sf_drive_config"]
    table_name = sf_config["table"]
    
    print(f"DEBUG: table_name: {table_name} ")
    
    # Build query directly in function - no need to separate
    query = f"""
    SELECT * FROM {table_name}
    WHERE 
    PIPELINE_STATUS = %(PIPELINE_STATUS)s 
    AND CONTINUITY_CHECK_PERFORMED = 'YES' 
    AND CAN_FETCH_HISTORICAL_DATA = 'YES' 
    AND PIPELINE_NAME = %(PIPELINE_NAME)s
    AND SOURCE_NAME = %(SOURCE_NAME)s
    AND SOURCE_CATEGORY = %(SOURCE_CATEGORY)s 
    AND SOURCE_SUB_TYPE = %(SOURCE_SUB_TYPE)s
    ORDER BY QUERY_WINDOW_START_TIME ASC
    """

    # Build params directly in function - no need to separate
    params = {
        'PIPELINE_STATUS': 'IN_PROCESS',
        'PIPELINE_NAME': config['PIPELINE_NAME'],
        'SOURCE_NAME': config['SOURCE_NAME'],
        'SOURCE_CATEGORY': config['SOURCE_CATEGORY'],
        'SOURCE_SUB_TYPE': config['SOURCE_SUB_TYPE']
    }
    print(f"DEBUG: Query : \n {query}")
    print(f"DEBUG: Params : \n {params}")
    print(f"DEBUG: Query and params built successfully")
    
    try:
        with get_snowflake_connection(sf_config, logger) as conn:
            with conn.cursor(DictCursor) as cursor:
                cursor.execute(query, params)
                results = cursor.fetchall()
                
                print(f"DEBUG: Query executed successfully - records_found: {len(results)} | query_id: {cursor.sfqid}")
                
                logger.info(
                    f"Query executed successfully with query_id: {cursor.sfqid}",
                    keyword="QUERY_EXECUTION_SUCCESS",
                    other_details={"query_id": cursor.sfqid, "records_found": len(results)}
                )
                
                print(f"DEBUG: find_in_process_records returning {len(results)} results")
                return results
            
    except Exception as e:
        # CRITICAL ERROR - Log query execution details with actual values
        logger.error(
            f"Find IN_PROCESS records operation failed: {str(e)}",
            keyword="FIND_IN_PROCESS_RECORDS_FAILED",
            other_details={
                "exception_type": str(type(e)),
                "exception_message": str(e),
                "table_name": f"value: {table_name} ",
                "query": query,
                # "params": {k: f"value: {v} | datatype: {type(v)}" for k, v in params.items()},
                "params": params,
                "sf_config_keys":  {k: ('******' if k == 'password' else f"value: {v}") for k, v in sf_config.items()}
            }
        )
        print(f"DEBUG: find_in_process_records failed - exception: {str(e)} | datatype: {type(e)}")
        raise


def get_record_before_delete(conn, table_name: str, pipeline_id: str, logger: CustomLogger) -> Dict[str, Any]:
    """Fetch and log the record before deletion for recovery purposes."""
    print(f"DEBUG: get_record_before_delete() called")
    print(f"DEBUG: table_name: {table_name}")
    print(f"DEBUG: pipeline_id: {pipeline_id}")
    
    select_query = f"SELECT * FROM {table_name} WHERE PIPELINE_ID = %(PIPELINE_ID)s"
    select_params = {'PIPELINE_ID': pipeline_id}
    
    try:
        with conn.cursor(DictCursor) as cursor:
            cursor.execute(select_query, select_params)
            records = cursor.fetchall()
            
            print(f"DEBUG: Records fetched - count: {len(records)} | query_id: {cursor.sfqid}")
            
            if len(records) == 0:
                # CRITICAL ERROR - No record found
                logger.error(
                    f"No record found for deletion - PIPELINE_ID: {pipeline_id}",
                    keyword="DELETE_RECORD_NOT_FOUND",
                    other_details={
                        "pipeline_id": pipeline_id,
                        "table_name": table_name,
                        "query_id": cursor.sfqid,
                        "records_found": len(records),
                        "select_query": f"SELECT * FROM {table_name} WHERE PIPELINE_ID = {pipeline_id}"
                    }
                )
                error_msg = f"No record found with PIPELINE_ID: {pipeline_id} - cannot delete non-existent record"
                # print(f"DEBUG: {error_msg}")
                raise Exception(error_msg)
                
            elif len(records) > 1:
                # CRITICAL ERROR - Data integrity issue
                logger.error(
                    f"Multiple records found for deletion - data integrity issue - PIPELINE_ID: {pipeline_id}",
                    keyword="DELETE_MULTIPLE_RECORDS_FOUND",
                    other_details={
                        "pipeline_id": pipeline_id,
                        "table_name": table_name,
                        "query_id": cursor.sfqid,
                        "records_found": len(records),
                        "duplicate_records": records,
                        "select_query": select_query
                    }
                )
                error_msg = f"Multiple records found with PIPELINE_ID: {pipeline_id} - data integrity issue"
                print(f"DEBUG: {error_msg}")
                raise Exception(error_msg)
            
            record_to_delete = records[0]
            print(f"DEBUG: Record found successfully - fields: {len(record_to_delete)}")
            
            # LOG THE COMPLETE RECORD FOR RECOVERY - THIS IS CRITICAL
            logger.info(
                f"RECORD ABOUT TO BE DELETED - PIPELINE_ID: {pipeline_id}",
                keyword="RECORD_BEFORE_DELETE",
                other_details={
                    "PIPELINE_ID": pipeline_id,
                    "table_name": table_name,
                    "complete_record": record_to_delete,
                    "query_id": cursor.sfqid,
                    "record_field_count": len(record_to_delete)
                }
            )
            
            return record_to_delete
            
    except Exception as e:
        # CRITICAL ERROR - Record fetch failed
        logger.error(
            f"Failed to fetch record before deletion - PIPELINE_ID: {pipeline_id}: {str(e)}",
            keyword="FETCH_BEFORE_DELETE_FAILED",
            other_details={
                "exception_message": str(e),
                "pipeline_id": pipeline_id,
                "table_name": table_name,
                "select_query": select_query
            }
        )
        print(f"DEBUG: get_record_before_delete failed - exception: {str(e)} | datatype: {type(e)}")
        raise


def execute_delete_query(conn, table_name: str, pipeline_id: str, logger: CustomLogger) -> str:
    """Execute DELETE query and return query ID."""
    print(f"DEBUG: execute_delete_query() called")
    print(f"DEBUG: table_name: {table_name} ")
    print(f"DEBUG: pipeline_id: {pipeline_id}")
    
    # SAFETY: Get and log the record before deletion
    record_to_delete = get_record_before_delete(conn, table_name, pipeline_id, logger)
    
    delete_query = f"DELETE FROM {table_name} WHERE PIPELINE_ID = %(PIPELINE_ID)s"
    delete_params = {'PIPELINE_ID': pipeline_id}
    
    try:
        with conn.cursor() as cursor:
            cursor.execute(delete_query, delete_params)
            delete_rows_affected = cursor.rowcount
            delete_query_id = cursor.sfqid
            
            print(f"DEBUG: DELETE executed - rows_affected: {delete_rows_affected} | query_id: {delete_query_id}")
            
            # Verify exactly one row was deleted
            if delete_rows_affected != 1:
                # CRITICAL ERROR - Unexpected row count
                logger.error(
                    f"DELETE operation affected unexpected number of rows - PIPELINE_ID: {pipeline_id}",
                    keyword="DELETE_UNEXPECTED_ROW_COUNT",
                    other_details={
                        "expected_rows": 1,
                        "actual_rows_affected": delete_rows_affected,
                        "pipeline_id": f"value: {pipeline_id}",
                        "table_name": f"value: {table_name} ",
                        "query_id": delete_query_id,
                        "delete_query": delete_query
                    }
                )
                error_msg = f"Expected to delete 1 row, but {delete_rows_affected} rows were affected for PIPELINE_ID: {pipeline_id}"
                print(f"DEBUG: {error_msg}")
                raise Exception(error_msg)
            
            logger.info(
                f"DELETE query executed successfully - PIPELINE_ID: {pipeline_id}",
                keyword="DELETE_RECORD_SUCCESS"
            )
            
            return delete_query_id
            
    except Exception as e:
        # CRITICAL ERROR - DELETE operation failed
        logger.error(
            f"DELETE query execution failed - PIPELINE_ID: {pipeline_id}: {str(e)}",
            keyword="DELETE_QUERY_FAILED",
            other_details={
                "exception_type": str(type(e)),
                "exception_message": str(e),
                "pipeline_id": f"value: {pipeline_id}",
                "table_name": f"value: {table_name} ",
                "delete_query": delete_query
            }
        )
        print(f"DEBUG: DELETE query failed - exception: {str(e)} | datatype: {type(e)}")
        raise


def execute_insert_query(conn, table_name: str, record_data: Dict[str, Any], logger: CustomLogger) -> str:
    """Execute INSERT query and return query ID."""
    print(f"DEBUG: execute_insert_query() called")
    print(f"DEBUG: table_name: {table_name}")
    print(f"DEBUG: record_data_fields: {len(record_data)} | datatype: {type(record_data)}")
    
    field_names = list(record_data.keys())
    pipeline_id = record_data.get('PIPELINE_ID', 'unknown')
    
    print(f"DEBUG: pipeline_id: {pipeline_id}")
    
    placeholders = [f"%({field})s" for field in field_names]
    insert_query = f"""
    INSERT INTO {table_name} ({', '.join(field_names)})
    VALUES ({', '.join(placeholders)})
    """
    
    insert_params = {field: record_data[field] for field in field_names}
    
    try:
        with conn.cursor() as cursor:
            cursor.execute(insert_query, insert_params)
            insert_rows_affected = cursor.rowcount
            insert_query_id = cursor.sfqid
            
            print(f"DEBUG: INSERT executed - rows_affected: {insert_rows_affected} | query_id: {insert_query_id}")
            
            # Verify exactly one row was inserted
            if insert_rows_affected != 1:
                # CRITICAL ERROR - Unexpected row count
                logger.error(
                    f"INSERT operation affected unexpected number of rows - PIPELINE_ID: {pipeline_id}",
                    keyword="INSERT_UNEXPECTED_ROW_COUNT",
                    other_details={
                        "expected_rows": 1,
                        "actual_rows_affected": insert_rows_affected,
                        "pipeline_id": f"value: {pipeline_id}",
                        "table_name": f"value: {table_name} ",
                        "query_id": insert_query_id,
                        "insert_query": insert_query,
                        "field_names": field_names
                    }
                )
                error_msg = f"Expected to insert 1 row, but {insert_rows_affected} rows were affected for PIPELINE_ID: {pipeline_id}"
                print(f"DEBUG: {error_msg}")
                raise Exception(error_msg)
            
            logger.info(
                f"INSERT query executed successfully",
                keyword="INSERT_RECORD_SUCCESS"
            )
            
            return insert_query_id
            
    except Exception as e:
        # CRITICAL ERROR - INSERT operation failed
        logger.error(
            f"INSERT query execution failed - PIPELINE_ID: {pipeline_id}: {str(e)}",
            keyword="INSERT_QUERY_FAILED",
            other_details={
                "exception_type": str(type(e)),
                "exception_message": str(e),
                "pipeline_id": f"value: {pipeline_id}",
                "table_name": f"value: {table_name} ",
                "insert_query": insert_query,
                "field_names": field_names,
                "record_data_sample": {k: f"value: {v} | datatype: {type(v)}" for k, v in list(record_data.items())[:5]}  # First 5 fields only
            }
        )
        print(f"DEBUG: INSERT query failed - exception: {str(e)} | datatype: {type(e)}")
        raise


def delete_single_record_from_snowflake(pipeline_id: str, config: Dict[str, Any], logger: CustomLogger) -> str:
    """Deletes a single record from Snowflake table."""
    print(f"DEBUG: delete_single_record_from_snowflake() called")
    print(f"DEBUG: pipeline_id: {pipeline_id}")
    # #  print(f"DEBUG: config: {type(config)}")
    
    validate_config_structure(config)
    validated_pipeline_id = validate_pipeline_id(pipeline_id)
    
    sf_config = config["sf_drive_config"]
    table_name = sf_config["table"]
    
    with get_snowflake_connection(sf_config, logger) as conn:
        result = execute_delete_query(conn, table_name, validated_pipeline_id, logger)
        print(f"DEBUG: delete_single_record_from_snowflake completed successfully")
        return result


def insert_single_record_to_snowflake(record_data: Dict[str, Any], config: Dict[str, Any], logger: CustomLogger) -> str:
    """Inserts a single record into Snowflake table."""
    print(f"DEBUG: insert_single_record_to_snowflake() called")
    print(f"""DEBUG: record_data: \n {record_data}""")
    
    validate_config_structure(config)
    
    if not record_data:
        error_msg = "record_data cannot be empty"
        print(f"DEBUG: {error_msg}")
        raise ValueError(error_msg)
    
    pipeline_id = record_data.get('PIPELINE_ID')
    print(f"DEBUG: inserting record \n {json.dumps(record_data, indent=4)}")
    print(f"DEBUG: extracted_pipeline_id: {pipeline_id}")
    
    validate_pipeline_id(pipeline_id)
    
    sf_config = config["sf_drive_config"]
    table_name = sf_config["table"]
    
    with get_snowflake_connection(sf_config, logger) as conn:
        result = execute_insert_query(conn, table_name, record_data, logger)
        print(f"DEBUG: insert_single_record_to_snowflake completed successfully")
        return result


def validate_record_pair(original_record: Dict[str, Any], updated_record: Dict[str, Any]) -> str:
    """Validate that both records have matching PIPELINE_IDs and return validated ID."""
    print(f"DEBUG: validate_record_pair() called")
    print(f"DEBUG: original_record: {type(original_record)}")
    
    original_id = original_record.get('PIPELINE_ID')
    updated_id = updated_record.get('PIPELINE_ID')

    print(f"DEBUG: original_id: {original_id}")
    print(f"DEBUG: updated_id: {updated_id}")

    validated_original = validate_pipeline_id(original_id)
    validated_updated = validate_pipeline_id(updated_id)
    
    if validated_original != validated_updated:
        # CRITICAL ERROR - Pipeline ID mismatch
        error_msg = f"PIPELINE_ID mismatch: original={validated_original}, updated={validated_updated}"
        print(f"DEBUG: {error_msg}")
        raise ValueError(error_msg)
    
    print(f"DEBUG: Record pair validation passed")
    return validated_original


def execute_transaction(conn, table_name: str, pipeline_id: str, updated_record: Dict[str, Any], logger: CustomLogger) -> tuple[str, str]:
    """Execute DELETE and INSERT in a transaction."""
    print(f"DEBUG: execute_transaction() called")
    print(f"DEBUG: table_name: {table_name}")
    print(f"DEBUG: pipeline_id: {pipeline_id}")
    print(f"DEBUG: updated_record: {json.dumps(updated_record, indent=4)}")
    
    # Start transaction
    conn.autocommit = False
    print(f"DEBUG: Transaction started (autocommit=False)")
    
    try:
        # Execute DELETE
        delete_query_id = execute_delete_query(conn, table_name, pipeline_id, logger)
        print(f"DEBUG: DELETE completed in transaction")
        
        # Execute INSERT
        insert_query_id = execute_insert_query(conn, table_name, updated_record, logger)
        print(f"DEBUG: INSERT completed in transaction")
        
        # Commit transaction
        conn.commit()
        conn.autocommit = True
        print(f"DEBUG: Transaction committed successfully")
        
        logger.info(
            f"Transaction committed successfully for PIPELINE_ID: {pipeline_id}",
            keyword="DELETE_INSERT_TRANSACTION_SUCCESS",
            other_details={
                        "pipeline_id": f"value: {pipeline_id}",
                        "table_name": f"value: {table_name} ",
                        "delete_query_id": delete_query_id,
                        "insert_query_id": insert_query_id,
                        "updated_record" : json.dumps(updated_record, indent=4)
                    }
        )
        
        return (delete_query_id, insert_query_id)
        
    except Exception as e:
        print(f"DEBUG: Transaction failed, attempting rollback")
        print(f"DEBUG: transaction_exception: {str(e)} | datatype: {type(e)}")
        
        try:
            conn.rollback()
            conn.autocommit = True
            print(f"DEBUG: Transaction rolled back successfully")
        except Exception as rollback_error:
            # CRITICAL ERROR - Rollback failed
            logger.error(
                f"Failed to rollback transaction for PIPELINE_ID: {pipeline_id}: {str(rollback_error)}",
                keyword="ROLLBACK_FAILED",
                other_details={
                    "original_exception": f"value: {str(e)}",
                    "rollback_exception": f"value: {str(rollback_error)}",
                    "pipeline_id": f"value: {pipeline_id}",
                    "table_name": f"value: {table_name} ",
                    "delete_query_id": delete_query_id,
                    "insert_query_id": insert_query_id,
                    "updated_record" : json.dumps(updated_record, indent=4)
                }
            )
            print(f"DEBUG: Rollback failed - exception: {str(rollback_error)} | datatype: {type(rollback_error)}")
        
        raise


def delete_old_in_process_record_and_insert_new_pending_record(
    original_stale_record: Dict[str, Any], 
    updated_stale_record: Dict[str, Any], 
    config: Dict[str, Any], 
    logger: CustomLogger
) -> None:
    """Deletes an old in-process record and inserts a new pending record in a transaction."""
    print(f"DEBUG: delete_old_in_process_record_and_insert_new_pending_record() called")
    print(f"DEBUG: original_stale_record: {json.dumps(original_stale_record, indent=4)} \n")
    print(f"DEBUG: updated_stale_record: {json.dumps(updated_stale_record, indent=4)} \n")
    # print(f"DEBUG: config: {json.dumps(config, indent=4)}")

    validate_config_structure(config)
    pipeline_id = validate_record_pair(original_stale_record, updated_stale_record)
    
    sf_config = config["sf_drive_config"]
    table_name = sf_config["table"]
    
    try:
        with get_snowflake_connection(sf_config, logger) as conn:
            delete_query_id, insert_query_id = execute_transaction(
                conn, table_name, pipeline_id, updated_stale_record, logger
            )
            print(f"DEBUG: Transaction completed successfully")
            
    except Exception as e:
        # CRITICAL ERROR - Transaction operation failed
        logger.error(
            f"DELETE and INSERT operation failed for PIPELINE_ID: {pipeline_id}: {str(e)}",
            keyword="DELETE_INSERT_RECORD_FAILED",
            other_details={
                "exception_message": str(e),
                "pipeline_id": f"value: {pipeline_id}",
                "table_name": f"value: {table_name}",
                "original_record_fields": json.dumps(original_stale_record, indent=4),
                "updated_record_fields": json.dumps(updated_stale_record, indent=4)
            }
        )
        raise


def update_in_process_single_record_to_pending_record(stale_record: Dict[str, Any], config: Dict[str, Any], logger: CustomLogger) -> None:
    """
    Placeholder function to update a single stale record to pending status in database.
    
    Args:
        stale_record: Single stale record dictionary (already modified in memory)
        config: Configuration dictionary containing database connection info
        logger: CustomLogger instance for logging
        
    Returns:
        None
        
    Raises:
        Exception: If database update fails
    """
    print(f"DEBUG: update_in_process_single_record_to_pending_record() called")
    print(f"DEBUG: stale_record: {type(stale_record)} | config: {type(config)}")
    
    pipeline_id = stale_record.get('PIPELINE_ID', 'unknown')
    pipeline_status = stale_record.get('PIPELINE_STATUS')
    retry_attempt = stale_record.get('RETRY_ATTEMPT_NUMBER')
    
    print(f"DEBUG: pipeline_id: {pipeline_id}")
    print(f"DEBUG: pipeline_status: {pipeline_status} | datatype: {type(pipeline_status)}")
    print(f"DEBUG: retry_attempt: {retry_attempt} | datatype: {type(retry_attempt)}")
    
    # TODO: Implement actual database update logic
    # This will build the UPDATE SQL statement and execute it
    # Using PIPELINE_ID as the WHERE clause identifier
    
    logger.info(
        "Placeholder function called - actual database update logic to be implemented",
        keyword="UPDATE_SINGLE_RECORD_PLACEHOLDER",
        other_details={
            "PIPELINE_ID": pipeline_id,
            "PIPELINE_STATUS": pipeline_status,
            "RETRY_ATTEMPT_NUMBER": retry_attempt
        }
    )
    
    print(f"DEBUG: update_in_process_single_record_to_pending_record completed")
    pass


def parse_duration_string_to_seconds(duration_string: str) -> int:
    """
    Safely converts duration strings like '2d3h9s' to total seconds.
    Accepts units: d (days), h (hours), m (minutes), s (seconds).

    Ignores unsupported units like 'w' or malformed patterns.

    Args:
        duration_string (str): Duration string (e.g., '2d3h9s', '6000s').

    Returns:
        int: Total number of seconds.

    Raises:
        ValueError: If no valid duration units are found.
    """
    print(f"DEBUG: parse_duration_string_to_seconds() called with: {duration_string}")

    if not isinstance(duration_string, str):
        raise ValueError(f"Invalid type for duration string: {type(duration_string)}. Expected str.")

    total_seconds = 0
    valid_unit_found = False

    pattern_map = {
        'd': 86400,
        'h': 3600,
        'm': 60,
        's': 1
    }

    try:
        for unit, multiplier in pattern_map.items():
            match = re.search(fr"(\d+){unit}", duration_string)
            if match:
                value = int(match.group(1))
                seconds = value * multiplier
                total_seconds += seconds
                valid_unit_found = True
                print(f"  Parsed {value}{unit} → {seconds} seconds")

        if not valid_unit_found:
            raise ValueError(f"No valid time units (d/h/m/s) found in string: '{duration_string}'")

        print(f"DEBUG: Total seconds parsed = {total_seconds}")
        return total_seconds

    except Exception as e:
        print(f"ERROR: Failed to parse duration string: '{duration_string}' | Exception: {str(e)}")
        raise


def get_valid_pending_records(config: Dict[str, Any], logger: CustomLogger) -> List[Dict[str, Any]]:
    """
    Fetches valid PENDING records based on QUERY_WINDOW time constraints.

    Criteria:
    - PIPELINE_STATUS = 'PENDING'
    - CONTINUITY_CHECK_PERFORMED = 'YES'
    - CAN_FETCH_HISTORICAL_DATA = 'YES'
    - QUERY_WINDOW_START_TIME and END_TIME <= MAX_ACCEPTED_TIME

    MAX_ACCEPTED_TIME = now(timezone) - (x_time_back + granularity)

    Args:
        config (Dict[str, Any]): Configuration dictionary
        logger (CustomLogger): Logger for critical events

    Returns:
        List[Dict[str, Any]]: List of valid PENDING records
    """

    print("DEBUG: get_valid_pending_records() called")
    print(f"DEBUG: config keys = {list(config.keys())}")

    # Extract and validate critical config values
    try:
        timezone_str = config.get("timezone", "UTC")
        tz = pytz.timezone(timezone_str)
        print(f"DEBUG: timezone set to {timezone_str}")
    except Exception as e:
        print(f"ERROR: Invalid timezone in config: {str(e)}")
        raise

    try:
        x_time_back_str = config["x_time_back"]
        granularity_str = config["granularity"]
        print(f"DEBUG: x_time_back = {x_time_back_str}, granularity = {granularity_str}")

        x_time_back_seconds = parse_duration_string_to_seconds(x_time_back_str)
        granularity_seconds = parse_duration_string_to_seconds(granularity_str)

        total_offset_seconds = x_time_back_seconds + granularity_seconds
        now = datetime.now(tz)
        max_accepted_time = now - timedelta(seconds=total_offset_seconds)
        print(f"DEBUG: MAX_ACCEPTED_TIME = {max_accepted_time.isoformat()}")

    except Exception as e:
        print(f"ERROR: Failed to calculate MAX_ACCEPTED_TIME: {str(e)}")
        raise

    # Prepare SQL
    table_name = config["sf_drive_config"]["table"]
    max_pending_records = config["max_pending_records"]
    query = f"""
    SELECT * FROM {table_name}
    WHERE 
        PIPELINE_STATUS = %(PIPELINE_STATUS)s 
        AND CONTINUITY_CHECK_PERFORMED = 'YES' 
        AND CAN_FETCH_HISTORICAL_DATA = 'YES' 
        AND PIPELINE_NAME = %(PIPELINE_NAME)s
        AND SOURCE_NAME = %(SOURCE_NAME)s
        AND SOURCE_CATEGORY = %(SOURCE_CATEGORY)s 
        AND SOURCE_SUB_TYPE = %(SOURCE_SUB_TYPE)s
        AND QUERY_WINDOW_START_TIME <= %(MAX_ACCEPTED_TIME)s
    ORDER BY QUERY_WINDOW_START_TIME ASC
    LIMIT {max_pending_records}
    """
    # AND QUERY_WINDOW_END_TIME <= %(MAX_ACCEPTED_TIME)s

    params = {
        'PIPELINE_STATUS': 'PENDING',
        'PIPELINE_NAME': config['PIPELINE_NAME'],
        'SOURCE_NAME': config['SOURCE_NAME'],
        'SOURCE_CATEGORY': config['SOURCE_CATEGORY'],
        'SOURCE_SUB_TYPE': config['SOURCE_SUB_TYPE'],
        'MAX_ACCEPTED_TIME': max_accepted_time
    }

    print("DEBUG: SQL and params ready. Executing query...")

    print("\nQuery Parameters:")
    for k, v in params.items():
        print(f"  {k} = {v}  (type: {type(v)})")

    print("\nFormatted SQL for manual testing:")
    formatted_query = query
    for key, val in params.items():
        raw_val = f"'{val}'" if isinstance(val, str) else f"TO_TIMESTAMP('{val.isoformat()}')" if isinstance(val, datetime) else str(val)
        formatted_query = formatted_query.replace(f"%({key})s", raw_val)
    print(formatted_query)

    try:
        with get_snowflake_connection(config["sf_drive_config"], logger) as conn:
            with conn.cursor(DictCursor) as cursor:
                cursor.execute(query, params)
                results = cursor.fetchall()

                print(f"DEBUG: Query executed. Records found = {len(results)} | Query ID: {cursor.sfqid}")
                logger.info(
                    "Fetched valid PENDING records successfully",
                    keyword="FETCH_VALID_PENDING_SUCCESS",
                    other_details={
                        "query_id": cursor.sfqid,
                        "records_found": len(results)
                    }
                )
                return results

    except Exception as e:
        print(f"ERROR: Failed to fetch valid pending records: {str(e)}")
        logger.error(
            "Exception while fetching valid pending records",
            keyword="FETCH_VALID_PENDING_FAILED",
            other_details={
                "exception_type": type(e).__name__,
                "exception_message": str(e),
                "table_name": table_name
            }
        )
        raise











