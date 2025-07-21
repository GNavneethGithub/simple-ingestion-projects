from typing import Dict, Any, bool
from source_scripts import test_source_connection
from stage_scripts import test_stage_connection  
from target_scripts import test_target_connection
from drive_scripts import test_drive_connection

def check_all_connections(config: Dict[str, Any],logger) -> Dict[str, bool]:
   """
   Validates connectivity to all pipeline systems before data processing begins.
   
   Purpose: Pre-flight connection validation for data pipeline execution
   
   Input: config (Dict[str, Any]) - Configuration dictionary containing connection 
          parameters, credentials, and system endpoints
   
   Output: Dict[str, bool] - Connection status dictionary with boolean flags:
           - is_source_connection_available
           - is_stage_connection_available  
           - is_target_connection_available
           - is_drive_connection_available
   """
  
   return {
       'is_source_connection_available': test_source_connection(config),
       'is_stage_connection_available': test_stage_connection(config), 
       'is_target_connection_available': test_target_connection(config),
       'is_drive_connection_available': test_drive_connection(config)
   }