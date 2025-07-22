import snowflake.connector
from snowflake.connector import DictCursor
from typing import Dict, Any, List
from custom_logger import CustomLogger

def get_snowflake_connection(sf_config: Dict[str, Any], logger: CustomLogger) -> snowflake.connector.SnowflakeConnection | None:
   """
   Creates and returns a Snowflake connection.
   
   Args:
       sf_config: Dictionary containing Snowflake credentials and config
       logger: CustomLogger instance for logging connection status
       
   Returns:
       Snowflake connection object
   """
   
   try:
       logger.info(
           "Attempting to establish Snowflake connection",
           keyword="SNOWFLAKE_CONNECTION_START"
       )
       
       conn = snowflake.connector.connect(
           account=sf_config['account'],
           user=sf_config['user'],
           password=sf_config['password'],
           warehouse=sf_config['warehouse'],
           database=sf_config['database'],
           schema=sf_config['schema']
       )
       
       logger.info(
           "Snowflake connection established successfully",
           keyword="SNOWFLAKE_CONNECTION_SUCCESS",
           other_details={
               "account": sf_config['account'],
               "database": sf_config['database'],
               "schema": sf_config['schema']
           }
       )
       
       return conn
       
   except Exception as e:
       logger.error(
           f"Failed to establish Snowflake connection: {str(e)}",
           keyword="SNOWFLAKE_CONNECTION_FAILED",
           other_details={"error": str(e)}
       )
       raise




def find_in_process_records(config: Dict[str, Any], logger: CustomLogger) -> List[Dict[str, Any]]:
    """
    Finds all records with in_process status from drive table.
    
    Args:
        config: Configuration dictionary containing sf_config
        logger: CustomLogger instance for logging
        
    Returns:
        List of dictionaries representing IN_PROCESS records
    """
    
    # Extract sf_config from config
    sf_config = config["sf_drive_config"]
    table_name = sf_config["table"]
    
    # Get Snowflake connection
    conn = get_snowflake_connection(sf_config, logger)
    
    # Build SQL query
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
    
    # Parameters for query (all uppercase keys from config)
    params = {
        'PIPELINE_STATUS': 'IN_PROCESS',
        'PIPELINE_NAME': config['PIPELINE_NAME'],
        'SOURCE_NAME': config['SOURCE_NAME'],
        'SOURCE_CATEGORY': config['SOURCE_CATEGORY'],
        'SOURCE_SUB_TYPE': config['SOURCE_SUB_TYPE']
    }
        
    # Log SQL query and parameters
    logger.info(
        message=query,
        keyword="FIND_IN_PROCESS_RECORDS",
        other_details=params
    )

    try:
        with conn.cursor(DictCursor) as cursor:
            cursor.execute(query, params)
            results = cursor.fetchall()
            
            # Log query ID and success
            logger.info(
                f"Find IN_PROCESS records query executed successfully with query_id: {cursor.sfqid}",
                keyword="FIND_IN_PROCESS_RECORDS",
                other_details={"query_id": cursor.sfqid, "records_found": len(results)}
            )
            
        return results
        
    except Exception as e:
        logger.error(
            f"Find IN_PROCESS records query failed: {str(e)}",
            keyword="FIND_IN_PROCESS_RECORDS",
            other_details={"error": str(e)}
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
    
    # TODO: Implement actual database update logic
    # This will build the UPDATE SQL statement and execute it
    # Using PIPELINE_ID as the WHERE clause identifier
    
    logger.info(
        "Placeholder function called - actual database update logic to be implemented",
        keyword="UPDATE_SINGLE_RECORD_PLACEHOLDER",
        other_details={
            "PIPELINE_ID": stale_record.get('PIPELINE_ID', 'unknown'),
            "PIPELINE_STATUS": stale_record.get('PIPELINE_STATUS'),
            "RETRY_ATTEMPT_NUMBER": stale_record.get('RETRY_ATTEMPT_NUMBER')
        }
    )
    
    pass


def delete_single_record_from_snowflake(pipeline_id: str, config: Dict[str, Any], logger: CustomLogger) -> str:
    """
    Deletes a single record from Snowflake table.
    
    Args:
        pipeline_id: PIPELINE_ID to identify the record to delete
        config: Configuration dictionary containing sf_drive_config
        logger: CustomLogger instance for logging
        
    Returns:
        Query ID of the DELETE operation
        
    Raises:
        Exception: If DELETE operation fails or affects unexpected number of rows
    """
    
    # Extract sf_config from config
    sf_config = config["sf_drive_config"]
    table_name = sf_config["table"]
    
    # Get Snowflake connection
    conn = get_snowflake_connection(sf_config, logger)
    
    delete_query = f"DELETE FROM {table_name} WHERE PIPELINE_ID = %(PIPELINE_ID)s"
    delete_params = {'PIPELINE_ID': pipeline_id}
    
    logger.info(
        f"Executing DELETE query for PIPELINE_ID: {pipeline_id}",
        keyword="DELETE_RECORD_SQL",
        other_details={
            "query": delete_query,
            "params": delete_params,
            "table_name": table_name
        }
    )
    
    try:
        with conn.cursor() as cursor:
            cursor.execute(delete_query, delete_params)
            delete_rows_affected = cursor.rowcount
            delete_query_id = cursor.sfqid
            
            logger.info(
                f"DELETE query executed: query_id={delete_query_id}, rows_affected={delete_rows_affected}",
                keyword="DELETE_RECORD_SUCCESS",
                other_details={
                    "query_id": delete_query_id,
                    "rows_affected": delete_rows_affected,
                    "PIPELINE_ID": pipeline_id,
                    "table_name": table_name
                }
            )
            
            # Verify exactly one row was deleted
            if delete_rows_affected != 1:
                raise Exception(f"Expected to delete 1 row, but {delete_rows_affected} rows were affected for PIPELINE_ID: {pipeline_id}")
        
        # Close connection
        conn.close()
        return delete_query_id
        
    except Exception as e:
        # Close connection on error
        if conn:
            conn.close()
            
        logger.error(
            f"DELETE operation failed for PIPELINE_ID: {pipeline_id}: {str(e)}",
            keyword="DELETE_RECORD_FAILED",
            other_details={
                "error": str(e),
                "PIPELINE_ID": pipeline_id,
                "table_name": table_name
            }
        )
        raise


def insert_single_record_to_snowflake(record_data: Dict[str, Any], config: Dict[str, Any], logger: CustomLogger) -> str:
    """
    Inserts a single record into Snowflake table.
    
    Args:
        record_data: Dictionary containing all field names and values to insert
        config: Configuration dictionary containing sf_drive_config
        logger: CustomLogger instance for logging
        
    Returns:
        Query ID of the INSERT operation
        
    Raises:
        Exception: If INSERT operation fails or affects unexpected number of rows
    """
    
    # Extract sf_config from config
    sf_config = config["sf_drive_config"]
    table_name = sf_config["table"]
    
    # Get Snowflake connection
    conn = get_snowflake_connection(sf_config, logger)
    
    # Get all field names from the record data
    field_names = list(record_data.keys())
    pipeline_id = record_data.get('PIPELINE_ID', 'unknown')
    
    # Build INSERT statement
    placeholders = [f"%({field})s" for field in field_names]
    insert_query = f"""
    INSERT INTO {table_name} ({', '.join(field_names)})
    VALUES ({', '.join(placeholders)})
    """
    
    # Prepare parameters (handle None values properly)
    insert_params = {}
    for field in field_names:
        value = record_data[field]
        insert_params[field] = value
    
    logger.info(
        f"Executing INSERT query for PIPELINE_ID: {pipeline_id}",
        keyword="INSERT_RECORD_SQL",
        other_details={
            "query": insert_query,
            "fields_count": len(field_names),
            "PIPELINE_ID": pipeline_id,
            "table_name": table_name
        }
    )
    
    try:
        with conn.cursor() as cursor:
            cursor.execute(insert_query, insert_params)
            insert_rows_affected = cursor.rowcount
            insert_query_id = cursor.sfqid
            
            logger.info(
                f"INSERT query executed: query_id={insert_query_id}, rows_affected={insert_rows_affected}",
                keyword="INSERT_RECORD_SUCCESS",
                other_details={
                    "query_id": insert_query_id,
                    "rows_affected": insert_rows_affected,
                    "PIPELINE_ID": pipeline_id,
                    "table_name": table_name
                }
            )
            
            # Verify exactly one row was inserted
            if insert_rows_affected != 1:
                raise Exception(f"Expected to insert 1 row, but {insert_rows_affected} rows were affected for PIPELINE_ID: {pipeline_id}")
        
        # Close connection
        conn.close()
        return insert_query_id
        
    except Exception as e:
        # Close connection on error
        if conn:
            conn.close()
            
        logger.error(
            f"INSERT operation failed for PIPELINE_ID: {pipeline_id}: {str(e)}",
            keyword="INSERT_RECORD_FAILED",
            other_details={
                "error": str(e),
                "PIPELINE_ID": pipeline_id,
                "table_name": table_name
            }
        )
        raise


def delete_old_in_process_record_and_insert_new_pending_record(original_stale_record: Dict[str, Any], updated_stale_record: Dict[str, Any], config: Dict[str, Any], logger: CustomLogger) -> None:
    """
    Deletes an old in-process record and inserts a new pending record in a transaction.
    
    Args:
        original_stale_record: Original stale record dictionary (for DELETE operation)
        updated_stale_record: Updated stale record dictionary (for INSERT operation)
        config: Configuration dictionary containing sf_drive_config
        logger: CustomLogger instance for logging
        
    Returns:
        None
        
    Raises:
        Exception: If database operations fail
    """
    
    # Extract sf_config from config
    sf_config = config["sf_drive_config"]
    table_name = sf_config["table"]
    
    # Get Snowflake connection
    conn = get_snowflake_connection(sf_config, logger)
    
    # Get PIPELINE_ID for operations
    pipeline_id = original_stale_record.get('PIPELINE_ID')
    if not pipeline_id:
        raise ValueError("PIPELINE_ID is required for DELETE operation")
    
    # Verify both records have the same PIPELINE_ID
    if updated_stale_record.get('PIPELINE_ID') != pipeline_id:
        raise ValueError(f"PIPELINE_ID mismatch: original={pipeline_id}, updated={updated_stale_record.get('PIPELINE_ID')}")
    
    delete_query_id = None
    insert_query_id = None
    
    try:
        # Start transaction
        conn.autocommit = False
        
        logger.info(
            f"Starting transaction for PIPELINE_ID: {pipeline_id}",
            keyword="DELETE_INSERT_TRANSACTION_START",
            other_details={"PIPELINE_ID": pipeline_id, "table_name": table_name}
        )
        
        with conn.cursor() as cursor:
            # Step 1: DELETE old record
            delete_query = f"DELETE FROM {table_name} WHERE PIPELINE_ID = %(PIPELINE_ID)s"
            delete_params = {'PIPELINE_ID': pipeline_id}
            
            logger.info(
                f"Executing DELETE query for PIPELINE_ID: {pipeline_id}",
                keyword="DELETE_RECORD_SQL",
                other_details={
                    "query": delete_query,
                    "params": delete_params,
                    "table_name": table_name
                }
            )
            
            cursor.execute(delete_query, delete_params)
            delete_rows_affected = cursor.rowcount
            delete_query_id = cursor.sfqid
            
            logger.info(
                f"DELETE query executed: query_id={delete_query_id}, rows_affected={delete_rows_affected}",
                keyword="DELETE_RECORD_SUCCESS",
                other_details={
                    "query_id": delete_query_id,
                    "rows_affected": delete_rows_affected,
                    "PIPELINE_ID": pipeline_id,
                    "table_name": table_name
                }
            )
            
            # Verify exactly one row was deleted
            if delete_rows_affected != 1:
                raise Exception(f"Expected to delete 1 row, but {delete_rows_affected} rows were affected for PIPELINE_ID: {pipeline_id}")
            
            # Step 2: INSERT new record
            field_names = list(updated_stale_record.keys())
            placeholders = [f"%({field})s" for field in field_names]
            insert_query = f"""
            INSERT INTO {table_name} ({', '.join(field_names)})
            VALUES ({', '.join(placeholders)})
            """
            
            insert_params = {}
            for field in field_names:
                value = updated_stale_record[field]
                insert_params[field] = value
            
            logger.info(
                f"Executing INSERT query for PIPELINE_ID: {pipeline_id}",
                keyword="INSERT_RECORD_SQL",
                other_details={
                    "query": insert_query,
                    "fields_count": len(field_names),
                    "PIPELINE_ID": pipeline_id,
                    "table_name": table_name
                }
            )
            
            cursor.execute(insert_query, insert_params)
            insert_rows_affected = cursor.rowcount
            insert_query_id = cursor.sfqid
            
            logger.info(
                f"INSERT query executed: query_id={insert_query_id}, rows_affected={insert_rows_affected}",
                keyword="INSERT_RECORD_SUCCESS",
                other_details={
                    "query_id": insert_query_id,
                    "rows_affected": insert_rows_affected,
                    "PIPELINE_ID": pipeline_id,
                    "table_name": table_name
                }
            )
            
            # Verify exactly one row was inserted
            if insert_rows_affected != 1:
                raise Exception(f"Expected to insert 1 row, but {insert_rows_affected} rows were affected for PIPELINE_ID: {pipeline_id}")
        
        # Commit transaction
        conn.commit()
        conn.autocommit = True
        
        logger.info(
            f"Transaction committed successfully for PIPELINE_ID: {pipeline_id}",
            keyword="DELETE_INSERT_TRANSACTION_SUCCESS",
            other_details={
                "PIPELINE_ID": pipeline_id,
                "table_name": table_name,
                "delete_query_id": delete_query_id,
                "insert_query_id": insert_query_id
            }
        )
        
        # Close connection
        conn.close()
        
    except Exception as e:
        # Rollback transaction on error
        try:
            conn.rollback()
            conn.autocommit = True
            logger.warning(
                f"Transaction rolled back due to error for PIPELINE_ID: {pipeline_id}",
                keyword="DELETE_INSERT_TRANSACTION_ROLLBACK",
                other_details={
                    "PIPELINE_ID": pipeline_id,
                    "table_name": table_name,
                    "error": str(e),
                    "delete_query_id": delete_query_id,
                    "insert_query_id": insert_query_id
                }
            )
        except Exception as rollback_error:
            logger.error(
                f"Failed to rollback transaction for PIPELINE_ID: {pipeline_id}: {str(rollback_error)}",
                keyword="DELETE_INSERT_ROLLBACK_FAILED",
                other_details={
                    "PIPELINE_ID": pipeline_id,
                    "table_name": table_name,
                    "rollback_error": str(rollback_error)
                }
            )
        
        # Close connection on error
        if conn:
            conn.close()
            
        logger.error(
            f"DELETE and INSERT operation failed for PIPELINE_ID: {pipeline_id}: {str(e)}",
            keyword="DELETE_INSERT_RECORD_FAILED",
            other_details={
                "error": str(e),
                "PIPELINE_ID": pipeline_id,
                "table_name": table_name,
                "delete_query_id": delete_query_id,
                "insert_query_id": insert_query_id
            }
        )
        raise


