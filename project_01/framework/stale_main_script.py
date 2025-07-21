from stale_detection_functions import find_in_progress_records, identify_stale_records, convert_to_pending
from email_alerts import send_stale_process_alert
from custom_logger import CustomLogger
from typing import Dict, Any, List

def detect_and_handle_stale_processes(config: Dict[str, Any], logger: CustomLogger) -> Dict[str, int]:
   """
   Detects stale in-progress records and converts them to pending status.
   
   Args:
       config: Configuration dictionary containing pipeline identifiers and thresholds
       logger: CustomLogger instance for structured logging
       
   Returns:
       Dict with counts: total_found, stale_found, converted_count
   """
   
   # Get all in-progress records
   in_progress_records: List[Dict[str, Any]] = find_in_progress_records(config)
   
   # Handle no records case
   if not in_progress_records:
       logger.info(
           f"No in-progress records found for pipeline: {config.get('pipeline_name')}, "
           f"source: {config.get('source_name')}, stage: {config.get('stage_name')}, "
           f"target: {config.get('target_name')}",
           keyword="NO_IN_PROGRESS_RECORDS"
       )
       return {"total_found": 0, "stale_found": 0, "converted_count": 0}
   
   # Log N in-progress records found
   logger.info(
       f"Found {len(in_progress_records)} in-progress records",
       keyword="IN_PROGRESS_RECORDS_FOUND",
       other_details={"count": len(in_progress_records)}
   )
   
   # Identify stale records
   stale_records: List[Dict[str, Any]] = identify_stale_records(in_progress_records, config)
   
   # Log M stale records identified
   logger.info(
       f"Found {len(stale_records)} stale records",
       keyword="STALE_RECORDS_IDENTIFIED",
       other_details={"stale_count": len(stale_records)}
   )
   
   # Send alert and convert stale records to pending
   if stale_records:
       send_stale_process_alert(stale_records, config)
       converted_count: int = convert_to_pending(stale_records, config)
       
       # Log M records converted to pending
       logger.info(
           f"Converted {converted_count} stale records to pending status",
           keyword="RECORDS_CONVERTED_TO_PENDING",
           other_details={"converted_count": converted_count}
       )
   else:
       converted_count = 0
   
   return {
       "total_found": len(in_progress_records),
       "stale_found": len(stale_records), 
       "converted_count": converted_count
   }



