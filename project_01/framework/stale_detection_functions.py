from drive_scripts import find_in_progress_records, delete_old_in_process_record_and_insert_new_pending_record
from email_alerts import send_stale_process_alert
from custom_logger import CustomLogger
from typing import Dict, Any, List
from datetime import datetime
import pytz
import re
import copy



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
   stale_records: List[Dict[str, Any]] = identify_stale_records(in_progress_records, config, logger)
   
   # Log M stale records identified
   logger.info(
       f"Found {len(stale_records)} stale records",
       keyword="STALE_RECORDS_IDENTIFIED",
       other_details={"stale_count": len(stale_records)}
   )
   
   # Send alert and convert stale records to pending
   if stale_records:
       # Make deep copy of original stale records before modification
       original_stale_records: List[Dict[str, Any]] = copy.deepcopy(stale_records)
       
       logger.info(
           f"Created deep copy of {len(original_stale_records)} stale records for database operations",
           keyword="STALE_RECORDS_COPY_CREATED",
           other_details={"original_records_count": len(original_stale_records)}
       )
       
       send_stale_process_alert(stale_records, config)
       converted_count: int = convert_to_pending(stale_records, original_stale_records, config, logger)
       
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




def convert_to_pending(stale_records: List[Dict[str, Any]], original_stale_records: List[Dict[str, Any]], config: Dict[str, Any], logger: CustomLogger) -> int:
   """
   Converts stale records to pending status by updating the Python dictionaries.
   
   Args:
       stale_records: List of stale record dictionaries (will be modified in-place)
       original_stale_records: Deep copy of original stale records (for database DELETE operation)
       config: Configuration dictionary
       logger: CustomLogger instance for logging
       
   Returns:
       Number of records successfully converted to pending
   """
   
   converted_count = 0
   
   for record in stale_records:
       try:
           # Phase 1: PRE_VALIDATION (always checked)
           if record.get('PHASE_COMPLETED') is None or record.get('PHASE_COMPLETED') == '':
               # Stuck in PRE_VALIDATION
               record['PIPELINE_STATUS'] = 'PENDING'
               record['PIPELINE_START_TIME'] = None
               record['PIPELINE_END_TIME'] = None
               record['PIPELINE_DURATION'] = None
           else:
               # Phase 2: SOURCE_TO_STAGE transfer (if enabled)
               if record.get('SRC_STG_XFER_ENABLED') == True:
                   if record.get('SRC_STG_XFER_STATUS') == 'IN_PROCESS':
                       # Stuck in SRC_STG transfer
                       record['SRC_STG_XFER_STATUS'] = 'PENDING'
                       record['SRC_STG_XFER_START_TS'] = None
                       record['SRC_STG_XFER_END_TS'] = None
                       record['SRC_STG_XFER_DURATION'] = None
                   else:
                       # Phase 3: SOURCE_TO_STAGE audit (if enabled)
                       if record.get('SRC_STG_AUDIT_ENABLED') == True:
                           if record.get('SRC_STG_AUDIT_STATUS') == 'IN_PROCESS':
                               # Stuck in SRC_STG audit
                               record['SRC_STG_AUDIT_STATUS'] = 'PENDING'
                               record['SRC_STG_AUDIT_START_TS'] = None
                               record['SRC_STG_AUDIT_END_TS'] = None
                               record['SRC_STG_AUDIT_DURATION'] = None
                           else:
                               # Phase 4: STAGE_TO_TARGET transfer (if enabled)
                               if record.get('STG_TGT_XFER_ENABLED') == True:
                                   if record.get('STG_TGT_XFER_STATUS') == 'IN_PROCESS':
                                       # Stuck in STG_TGT transfer
                                       record['STG_TGT_XFER_STATUS'] = 'PENDING'
                                       record['STG_TGT_XFER_START_TS'] = None
                                       record['STG_TGT_XFER_END_TS'] = None
                                       record['STG_TGT_XFER_DURATION'] = None
                                   else:
                                       # Phase 5: STAGE_TO_TARGET audit (if enabled)
                                       if record.get('STG_TGT_AUDIT_ENABLED') == True:
                                           if record.get('STG_TGT_AUDIT_STATUS') == 'IN_PROCESS':
                                               # Stuck in STG_TGT audit
                                               record['STG_TGT_AUDIT_STATUS'] = 'PENDING'
                                               record['STG_TGT_AUDIT_START_TS'] = None
                                               record['STG_TGT_AUDIT_END_TS'] = None
                                               record['STG_TGT_AUDIT_DURATION'] = None
                                           else:
                                               # Phase 6: SOURCE_TO_TARGET audit (if enabled)
                                               if record.get('SRC_TGT_AUDIT_ENABLED') == True:
                                                   if record.get('SRC_TGT_AUDIT_STATUS') == 'IN_PROCESS':
                                                       # Stuck in SRC_TGT audit
                                                       record['SRC_TGT_AUDIT_STATUS'] = 'PENDING'
                                                       record['SRC_TGT_AUDIT_START_TS'] = None
                                                       record['SRC_TGT_AUDIT_END_TS'] = None
                                                       record['SRC_TGT_AUDIT_DURATION'] = None
                                               # If SRC_TGT_AUDIT disabled, we're done
                                       # If STG_TGT_AUDIT disabled, skip to SRC_TGT_AUDIT
                                       else:
                                           if record.get('SRC_TGT_AUDIT_ENABLED') == True:
                                               if record.get('SRC_TGT_AUDIT_STATUS') == 'IN_PROCESS':
                                                   record['SRC_TGT_AUDIT_STATUS'] = 'PENDING'
                                                   record['SRC_TGT_AUDIT_START_TS'] = None
                                                   record['SRC_TGT_AUDIT_END_TS'] = None
                                                   record['SRC_TGT_AUDIT_DURATION'] = None
                               # If STG_TGT_XFER disabled, skip to audits
                               else:
                                   if record.get('STG_TGT_AUDIT_ENABLED') == True:
                                       if record.get('STG_TGT_AUDIT_STATUS') == 'IN_PROCESS':
                                           record['STG_TGT_AUDIT_STATUS'] = 'PENDING'
                                           record['STG_TGT_AUDIT_START_TS'] = None
                                           record['STG_TGT_AUDIT_END_TS'] = None
                                           record['STG_TGT_AUDIT_DURATION'] = None
                                       else:
                                           if record.get('SRC_TGT_AUDIT_ENABLED') == True:
                                               if record.get('SRC_TGT_AUDIT_STATUS') == 'IN_PROCESS':
                                                   record['SRC_TGT_AUDIT_STATUS'] = 'PENDING'
                                                   record['SRC_TGT_AUDIT_START_TS'] = None
                                                   record['SRC_TGT_AUDIT_END_TS'] = None
                                                   record['SRC_TGT_AUDIT_DURATION'] = None
                                   else:
                                       if record.get('SRC_TGT_AUDIT_ENABLED') == True:
                                           if record.get('SRC_TGT_AUDIT_STATUS') == 'IN_PROCESS':
                                               record['SRC_TGT_AUDIT_STATUS'] = 'PENDING'
                                               record['SRC_TGT_AUDIT_START_TS'] = None
                                               record['SRC_TGT_AUDIT_END_TS'] = None
                                               record['SRC_TGT_AUDIT_DURATION'] = None
                       # If SRC_STG_AUDIT disabled, skip to STG_TGT_XFER
                       else:
                           if record.get('STG_TGT_XFER_ENABLED') == True:
                               if record.get('STG_TGT_XFER_STATUS') == 'IN_PROCESS':
                                   record['STG_TGT_XFER_STATUS'] = 'PENDING'
                                   record['STG_TGT_XFER_START_TS'] = None
                                   record['STG_TGT_XFER_END_TS'] = None
                                   record['STG_TGT_XFER_DURATION'] = None
                               else:
                                   if record.get('STG_TGT_AUDIT_ENABLED') == True:
                                       if record.get('STG_TGT_AUDIT_STATUS') == 'IN_PROCESS':
                                           record['STG_TGT_AUDIT_STATUS'] = 'PENDING'
                                           record['STG_TGT_AUDIT_START_TS'] = None
                                           record['STG_TGT_AUDIT_END_TS'] = None
                                           record['STG_TGT_AUDIT_DURATION'] = None
                                       else:
                                           if record.get('SRC_TGT_AUDIT_ENABLED') == True:
                                               if record.get('SRC_TGT_AUDIT_STATUS') == 'IN_PROCESS':
                                                   record['SRC_TGT_AUDIT_STATUS'] = 'PENDING'
                                                   record['SRC_TGT_AUDIT_START_TS'] = None
                                                   record['SRC_TGT_AUDIT_END_TS'] = None
                                                   record['SRC_TGT_AUDIT_DURATION'] = None
                                   else:
                                       if record.get('SRC_TGT_AUDIT_ENABLED') == True:
                                           if record.get('SRC_TGT_AUDIT_STATUS') == 'IN_PROCESS':
                                               record['SRC_TGT_AUDIT_STATUS'] = 'PENDING'
                                               record['SRC_TGT_AUDIT_START_TS'] = None
                                               record['SRC_TGT_AUDIT_END_TS'] = None
                                               record['SRC_TGT_AUDIT_DURATION'] = None
                           else:
                               # Both transfers disabled, check remaining audits
                               if record.get('STG_TGT_AUDIT_ENABLED') == True:
                                   if record.get('STG_TGT_AUDIT_STATUS') == 'IN_PROCESS':
                                       record['STG_TGT_AUDIT_STATUS'] = 'PENDING'
                                       record['STG_TGT_AUDIT_START_TS'] = None
                                       record['STG_TGT_AUDIT_END_TS'] = None
                                       record['STG_TGT_AUDIT_DURATION'] = None
                                   else:
                                       if record.get('SRC_TGT_AUDIT_ENABLED') == True:
                                           if record.get('SRC_TGT_AUDIT_STATUS') == 'IN_PROCESS':
                                               record['SRC_TGT_AUDIT_STATUS'] = 'PENDING'
                                               record['SRC_TGT_AUDIT_START_TS'] = None
                                               record['SRC_TGT_AUDIT_END_TS'] = None
                                               record['SRC_TGT_AUDIT_DURATION'] = None
                               else:
                                   if record.get('SRC_TGT_AUDIT_ENABLED') == True:
                                       if record.get('SRC_TGT_AUDIT_STATUS') == 'IN_PROCESS':
                                           record['SRC_TGT_AUDIT_STATUS'] = 'PENDING'
                                           record['SRC_TGT_AUDIT_START_TS'] = None
                                           record['SRC_TGT_AUDIT_END_TS'] = None
                                           record['SRC_TGT_AUDIT_DURATION'] = None
               # If SRC_STG_XFER disabled, skip to SRC_STG_AUDIT or next phases
               else:
                   if record.get('SRC_STG_AUDIT_ENABLED') == True:
                       if record.get('SRC_STG_AUDIT_STATUS') == 'IN_PROCESS':
                           record['SRC_STG_AUDIT_STATUS'] = 'PENDING'
                           record['SRC_STG_AUDIT_START_TS'] = None
                           record['SRC_STG_AUDIT_END_TS'] = None
                           record['SRC_STG_AUDIT_DURATION'] = None
                       else:
                           # Continue with STG_TGT phases...
                           if record.get('STG_TGT_XFER_ENABLED') == True:
                               if record.get('STG_TGT_XFER_STATUS') == 'IN_PROCESS':
                                   record['STG_TGT_XFER_STATUS'] = 'PENDING'
                                   record['STG_TGT_XFER_START_TS'] = None
                                   record['STG_TGT_XFER_END_TS'] = None
                                   record['STG_TGT_XFER_DURATION'] = None
                               else:
                                   if record.get('STG_TGT_AUDIT_ENABLED') == True:
                                       if record.get('STG_TGT_AUDIT_STATUS') == 'IN_PROCESS':
                                           record['STG_TGT_AUDIT_STATUS'] = 'PENDING'
                                           record['STG_TGT_AUDIT_START_TS'] = None
                                           record['STG_TGT_AUDIT_END_TS'] = None
                                           record['STG_TGT_AUDIT_DURATION'] = None
                                       else:
                                           if record.get('SRC_TGT_AUDIT_ENABLED') == True:
                                               if record.get('SRC_TGT_AUDIT_STATUS') == 'IN_PROCESS':
                                                   record['SRC_TGT_AUDIT_STATUS'] = 'PENDING'
                                                   record['SRC_TGT_AUDIT_START_TS'] = None
                                                   record['SRC_TGT_AUDIT_END_TS'] = None
                                                   record['SRC_TGT_AUDIT_DURATION'] = None
                                   else:
                                       if record.get('SRC_TGT_AUDIT_ENABLED') == True:
                                           if record.get('SRC_TGT_AUDIT_STATUS') == 'IN_PROCESS':
                                               record['SRC_TGT_AUDIT_STATUS'] = 'PENDING'
                                               record['SRC_TGT_AUDIT_START_TS'] = None
                                               record['SRC_TGT_AUDIT_END_TS'] = None
                                               record['SRC_TGT_AUDIT_DURATION'] = None
                           else:
                               # Continue with remaining audits...
                               if record.get('STG_TGT_AUDIT_ENABLED') == True:
                                   if record.get('STG_TGT_AUDIT_STATUS') == 'IN_PROCESS':
                                       record['STG_TGT_AUDIT_STATUS'] = 'PENDING'
                                       record['STG_TGT_AUDIT_START_TS'] = None
                                       record['STG_TGT_AUDIT_END_TS'] = None
                                       record['STG_TGT_AUDIT_DURATION'] = None
                                   else:
                                       if record.get('SRC_TGT_AUDIT_ENABLED') == True:
                                           if record.get('SRC_TGT_AUDIT_STATUS') == 'IN_PROCESS':
                                               record['SRC_TGT_AUDIT_STATUS'] = 'PENDING'
                                               record['SRC_TGT_AUDIT_START_TS'] = None
                                               record['SRC_TGT_AUDIT_END_TS'] = None
                                               record['SRC_TGT_AUDIT_DURATION'] = None
                               else:
                                   if record.get('SRC_TGT_AUDIT_ENABLED') == True:
                                       if record.get('SRC_TGT_AUDIT_STATUS') == 'IN_PROCESS':
                                           record['SRC_TGT_AUDIT_STATUS'] = 'PENDING'
                                           record['SRC_TGT_AUDIT_START_TS'] = None
                                           record['SRC_TGT_AUDIT_END_TS'] = None
                                           record['SRC_TGT_AUDIT_DURATION'] = None
                   else:
                       # SRC_STG_AUDIT also disabled, continue with STG_TGT phases...
                       if record.get('STG_TGT_XFER_ENABLED') == True:
                           if record.get('STG_TGT_XFER_STATUS') == 'IN_PROCESS':
                               record['STG_TGT_XFER_STATUS'] = 'PENDING'
                               record['STG_TGT_XFER_START_TS'] = None
                               record['STG_TGT_XFER_END_TS'] = None
                               record['STG_TGT_XFER_DURATION'] = None
                           else:
                               if record.get('STG_TGT_AUDIT_ENABLED') == True:
                                   if record.get('STG_TGT_AUDIT_STATUS') == 'IN_PROCESS':
                                       record['STG_TGT_AUDIT_STATUS'] = 'PENDING'
                                       record['STG_TGT_AUDIT_START_TS'] = None
                                       record['STG_TGT_AUDIT_END_TS'] = None
                                       record['STG_TGT_AUDIT_DURATION'] = None
                                   else:
                                       if record.get('SRC_TGT_AUDIT_ENABLED') == True:
                                           if record.get('SRC_TGT_AUDIT_STATUS') == 'IN_PROCESS':
                                               record['SRC_TGT_AUDIT_STATUS'] = 'PENDING'
                                               record['SRC_TGT_AUDIT_START_TS'] = None
                                               record['SRC_TGT_AUDIT_END_TS'] = None
                                               record['SRC_TGT_AUDIT_DURATION'] = None
                               else:
                                   if record.get('SRC_TGT_AUDIT_ENABLED') == True:
                                       if record.get('SRC_TGT_AUDIT_STATUS') == 'IN_PROCESS':
                                           record['SRC_TGT_AUDIT_STATUS'] = 'PENDING'
                                           record['SRC_TGT_AUDIT_START_TS'] = None
                                           record['SRC_TGT_AUDIT_END_TS'] = None
                                           record['SRC_TGT_AUDIT_DURATION'] = None
                       else:
                           # Most transfer phases disabled, check remaining audits
                           if record.get('STG_TGT_AUDIT_ENABLED') == True:
                               if record.get('STG_TGT_AUDIT_STATUS') == 'IN_PROCESS':
                                   record['STG_TGT_AUDIT_STATUS'] = 'PENDING'
                                   record['STG_TGT_AUDIT_START_TS'] = None
                                   record['STG_TGT_AUDIT_END_TS'] = None
                                   record['STG_TGT_AUDIT_DURATION'] = None
                               else:
                                   if record.get('SRC_TGT_AUDIT_ENABLED') == True:
                                       if record.get('SRC_TGT_AUDIT_STATUS') == 'IN_PROCESS':
                                           record['SRC_TGT_AUDIT_STATUS'] = 'PENDING'
                                           record['SRC_TGT_AUDIT_START_TS'] = None
                                           record['SRC_TGT_AUDIT_END_TS'] = None
                                           record['SRC_TGT_AUDIT_DURATION'] = None
                           else:
                               if record.get('SRC_TGT_AUDIT_ENABLED') == True:
                                   if record.get('SRC_TGT_AUDIT_STATUS') == 'IN_PROCESS':
                                       record['SRC_TGT_AUDIT_STATUS'] = 'PENDING'
                                       record['SRC_TGT_AUDIT_START_TS'] = None
                                       record['SRC_TGT_AUDIT_END_TS'] = None
                                       record['SRC_TGT_AUDIT_DURATION'] = None
           
           # Always update PIPELINE_STATUS and reset PIPELINE_START_TIME for all cases
           record['PIPELINE_STATUS'] = 'PENDING'
           record['PIPELINE_START_TIME'] = None
           
           # Increment retry_attempt_number
           current_retry = record.get('RETRY_ATTEMPT_NUMBER', 0) or 0
           record['RETRY_ATTEMPT_NUMBER'] = current_retry + 1
           
           converted_count += 1
           
       except Exception as e:
           logger.error(
               f"Failed to convert record to pending: {str(e)}",
               keyword="CONVERT_TO_PENDING",
               other_details={
                   "error": str(e),
                   "PIPELINE_ID": record.get('PIPELINE_ID', 'unknown'),
                   "PIPELINE_NAME": record.get('PIPELINE_NAME', 'unknown'),
                   "SOURCE_CATEGORY": record.get('SOURCE_CATEGORY', 'unknown'),
                   "SOURCE_SUB_TYPE": record.get('SOURCE_SUB_TYPE', 'unknown')
               }
           )
           continue
   
   # Delete old records and insert new pending records in database
   delete_old_in_process_records_and_insert_new_pending_record(original_stale_records, stale_records, config, logger)
   
   logger.info(
       f"Successfully converted {converted_count} records to pending status",
       keyword="CONVERT_TO_PENDING"
   )
   
   return converted_count







def delete_old_in_process_records_and_insert_new_pending_record(original_stale_records: List[Dict[str, Any]], updated_stale_records: List[Dict[str, Any]], config: Dict[str, Any], logger: CustomLogger) -> int:
    """
    Deletes old in-process records and inserts new pending records in the database.
    
    Args:
        original_stale_records: List of original stale record dictionaries (for DELETE operation)
        updated_stale_records: List of updated stale record dictionaries (for INSERT operation)
        config: Configuration dictionary containing database connection info
        logger: CustomLogger instance for logging
        
    Returns:
        Number of records successfully processed (deleted and inserted)
    """
    
    if not original_stale_records or not updated_stale_records:
        logger.info(
            "No stale records to process in database",
            keyword="DELETE_INSERT_RECORDS",
            other_details={
                "original_records_count": len(original_stale_records) if original_stale_records else 0,
                "updated_records_count": len(updated_stale_records) if updated_stale_records else 0
            }
        )
        return 0
    
    if len(original_stale_records) != len(updated_stale_records):
        raise ValueError(f"Mismatch in record counts: original={len(original_stale_records)}, updated={len(updated_stale_records)}")
    
    logger.info(
        f"Starting DELETE and INSERT operations for {len(original_stale_records)} stale records",
        keyword="DELETE_INSERT_RECORDS_START",
        other_details={"total_records": len(original_stale_records)}
    )
    
    successfully_processed = 0
    failed_operations = 0
    
    for i, (original_record, updated_record) in enumerate(zip(original_stale_records, updated_stale_records), 1):
        try:
            logger.info(
                f"Processing record {i}/{len(original_stale_records)} - DELETE old and INSERT new",
                keyword="DELETE_INSERT_SINGLE_RECORD_START",
                other_details={
                    "record_number": i,
                    "total_records": len(original_stale_records),
                    "PIPELINE_ID": original_record.get('PIPELINE_ID', 'unknown'),
                    "PIPELINE_NAME": original_record.get('PIPELINE_NAME', 'unknown')
                }
            )
            
            # Call the single record function
            delete_old_in_process_record_and_insert_new_pending_record(original_record, updated_record, config, logger)
            
            successfully_processed += 1
            
            logger.info(
                f"Successfully processed record {i}/{len(original_stale_records)}",
                keyword="DELETE_INSERT_SINGLE_RECORD_SUCCESS",
                other_details={
                    "record_number": i,
                    "PIPELINE_ID": original_record.get('PIPELINE_ID', 'unknown')
                }
            )
            
        except Exception as e:
            failed_operations += 1
            
            logger.error(
                f"Failed to process record {i}/{len(original_stale_records)}: {str(e)}",
                keyword="DELETE_INSERT_SINGLE_RECORD_FAILED",
                other_details={
                    "record_number": i,
                    "error": str(e),
                    "PIPELINE_ID": original_record.get('PIPELINE_ID', 'unknown'),
                    "PIPELINE_NAME": original_record.get('PIPELINE_NAME', 'unknown'),
                    "SOURCE_CATEGORY": original_record.get('SOURCE_CATEGORY', 'unknown'),
                    "SOURCE_SUB_TYPE": original_record.get('SOURCE_SUB_TYPE', 'unknown')
                }
            )
            # Continue with next record instead of stopping the whole batch
            continue
    
    # Log final results
    logger.info(
        f"DELETE and INSERT operations completed: {successfully_processed} successful, {failed_operations} failed",
        keyword="DELETE_INSERT_RECORDS_COMPLETE",
        other_details={
            "total_records": len(original_stale_records),
            "successful_operations": successfully_processed,
            "failed_operations": failed_operations,
            "success_rate": round((successfully_processed / len(original_stale_records)) * 100, 2) if len(original_stale_records) > 0 else 0
        }
    )
    
    return successfully_processed





