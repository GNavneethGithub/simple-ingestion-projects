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







