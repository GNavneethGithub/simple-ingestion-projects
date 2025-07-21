from typing import Dict, Any, bool
from source_scripts import test_source_connection
from stage_scripts import test_stage_connection  
from target_scripts import test_target_connection
from drive_scripts import test_drive_connection
from custom_logger import CustomLogger

def check_all_connections(config: Dict[str, Any], logger: CustomLogger) -> Dict[str, bool]:
   """
   Validates connectivity to all pipeline systems before data processing begins.
   
   Purpose: Pre-flight connection validation for data pipeline execution
   
   Input: config (Dict[str, Any]) - Configuration dictionary containing connection 
          parameters, credentials, and system endpoints
          logger (CustomLogger) - Logger instance for recording connection test results
   
   Output: Dict[str, bool] - Connection status dictionary with boolean flags:
           - is_source_connection_available
           - is_stage_connection_available  
           - is_target_connection_available
           - is_drive_connection_available
   """
   
   logger.info(
       "Starting connection health check for all pipeline systems",
       keyword="HEALTH_CHECK_START"
   )
   
   # Test each connection and log results
   source_status: bool = test_source_connection(config)
   logger.info(
       f"Source connection test: {'PASSED' if source_status else 'FAILED'}",
       keyword="SOURCE_CONNECTION_TEST",
       other_details={"status": source_status}
   )
   
   stage_status: bool = test_stage_connection(config)
   logger.info(
       f"Stage connection test: {'PASSED' if stage_status else 'FAILED'}",
       keyword="STAGE_CONNECTION_TEST", 
       other_details={"status": stage_status}
   )
   
   target_status: bool = test_target_connection(config)
   logger.info(
       f"Target connection test: {'PASSED' if target_status else 'FAILED'}",
       keyword="TARGET_CONNECTION_TEST",
       other_details={"status": target_status}
   )
   
   drive_status: bool = test_drive_connection(config)
   logger.info(
       f"Drive connection test: {'PASSED' if drive_status else 'FAILED'}",
       keyword="DRIVE_CONNECTION_TEST",
       other_details={"status": drive_status}
   )
   
   result: Dict[str, bool] = {
       'is_source_connection_available': source_status,
       'is_stage_connection_available': stage_status, 
       'is_target_connection_available': target_status,
       'is_drive_connection_available': drive_status
   }
   
   logger.info(
       "Connection health check completed",
       keyword="HEALTH_CHECK_COMPLETE",
       other_details=result
   )
   
   return result