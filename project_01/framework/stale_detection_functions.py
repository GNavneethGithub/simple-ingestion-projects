from drive_scripts import find_in_progress_records
from email_alerts import send_stale_process_alert
from custom_logger import CustomLogger
from typing import Dict, Any, List
from datetime import datetime
import pytz

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



# identify_stale_records, convert_to_pending
import re

def parse_duration_to_seconds(duration_str: str) -> int:
   """
   Parses duration string like "1d3h30m40s", "1d", "4h", "2h45m" to total seconds.
   
   Args:
       duration_str: Duration string in format like "1d3h30m40s"
       
   Returns:
       Total duration in seconds as integer
   """
   
   try:
       total_seconds = 0
       
       # Extract days
       days_match = re.search(r'(\d+)d', duration_str)
       if days_match:
           total_seconds += int(days_match.group(1)) * 24 * 60 * 60
       
       # Extract hours
       hours_match = re.search(r'(\d+)h', duration_str)
       if hours_match:
           total_seconds += int(hours_match.group(1)) * 60 * 60
       
       # Extract minutes
       minutes_match = re.search(r'(\d+)m', duration_str)
       if minutes_match:
           total_seconds += int(minutes_match.group(1)) * 60
       
       # Extract seconds
       seconds_match = re.search(r'(\d+)s', duration_str)
       if seconds_match:
           total_seconds += int(seconds_match.group(1))
       
       # If no valid units found, raise error
       if total_seconds == 0:
           raise ValueError(f"No valid duration units found in '{duration_str}'")
       
       return total_seconds
       
   except Exception as e:
       raise ValueError(f"Failed to parse duration string '{duration_str}': {str(e)}")





def identify_stale_records(list_of_python_dicts: List[Dict[str, Any]], config: Dict[str, Any], logger: CustomLogger) -> List[Dict[str, Any]]:
    """
    Identifies stale records from the list of in-progress records.

    Args:
        list_of_python_dicts: List of in-progress records from find_in_progress_records()
        config: Configuration dictionary 
        logger: CustomLogger instance for logging
        
    Returns:
        List of dictionaries containing only the stale records
    """

    stale_records = []
    timezone = config["timezone"]
    stale_threshold_factor = config["stale_threshold_factor"]
    tz = pytz.timezone(timezone)
    current_time = datetime.now(tz)  # Get current time in specified timezone

    for record in list_of_python_dicts:
        try:
            # Get expected duration - from record first, fallback to config
            expected_duration_str = record.get('PIPELINE_EXP_DURATION') or config['PIPELINE_EXP_DURATION']
            
            # Parse duration string to seconds using separate parsing function
            expected_duration_seconds = parse_duration_to_seconds(expected_duration_str)
            
            # Parse PIPELINE_START_TIME ISO string to datetime
            start_time_str = record['PIPELINE_START_TIME']
            start_time = datetime.fromisoformat(start_time_str)
            
            # Calculate actual duration in seconds
            actual_duration = (current_time - start_time).total_seconds()
            
            # Check if actual_duration > stale_threshold_factor * expected_duration_seconds
            threshold_seconds = stale_threshold_factor * expected_duration_seconds
            
            if actual_duration > threshold_seconds:
                stale_records.append(record)
                
        except Exception as e:
            # Log parsing errors but continue processing other records
            logger.warning(
                f"Failed to process record for staleness check: {str(e)}",
                keyword="IDENTIFY_STALE_RECORDS",
                other_details={
                "error": str(e), 
                "PIPELINE_ID": record.get('PIPELINE_ID', 'unknown'),
                "PIPELINE_NAME": record.get('PIPELINE_NAME', 'unknown'),
                "SOURCE_CATEGORY": record.get('SOURCE_CATEGORY', 'unknown'),
                "SOURCE_SUB_TYPE": record.get('SOURCE_SUB_TYPE', 'unknown')}
            )          
            continue

    # Log results
    logger.info(
        f"Identified {len(stale_records)} stale records out of {len(list_of_python_dicts)} in-progress records",
        keyword="IDENTIFY_STALE_RECORDS",
        other_details={"total_records": len(list_of_python_dicts), "stale_count": len(stale_records)}
    )

    return stale_records










