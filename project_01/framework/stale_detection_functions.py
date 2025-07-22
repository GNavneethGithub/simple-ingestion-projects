from datetime import datetime
from typing import Dict, Any, List
import pytz
import re
import copy

# we need these to run this script file
from drive_scripts import find_in_progress_records, delete_old_in_process_record_and_insert_new_pending_record
from email_alerts import send_stale_process_alert
from custom_logger import CustomLogger


def detect_and_handle_stale_records(config: Dict[str, Any], logger: CustomLogger) -> Dict[str, int]:
    """
    Main entry point to detect and handle stale in-progress records.
    """

    print("STEP 1: Fetching in-progress records...")
    in_progress_records = get_in_progress_records(config, logger)
    print(f"INFO: Total in-progress records found = {len(in_progress_records)}")

    if not in_progress_records:
        logger.info("No in-progress records found.", keyword="NO_IN_PROGRESS")
        return {"total_found": 0, "stale_found": 0, "converted_count": 0}

    print("STEP 2: Identifying stale records...")
    stale_records = identify_stale_records_from_list(in_progress_records, config, logger)
    print(f"INFO: Total stale records identified = {len(stale_records)}")

    if not stale_records:
        print("No stale records found to process.")
        return {"total_found": len(in_progress_records), "stale_found": 0, "converted_count": 0}

    print("STEP 3: Sending alert for stale records...")
    send_stale_process_alert(stale_records, config)

    print("STEP 4: Converting stale records to pending...")
    original_records = copy.deepcopy(stale_records)
    converted_count = convert_all_stale_to_pending(stale_records, original_records, config, logger)

    print(f"DONE: Total converted to pending = {converted_count}")

    return {
        "total_found": len(in_progress_records),
        "stale_found": len(stale_records),
        "converted_count": converted_count
    }


def get_in_progress_records(config: Dict[str, Any], logger: CustomLogger) -> List[Dict[str, Any]]:
    print("Calling find_in_progress_records()...")
    return find_in_progress_records(config)


def identify_stale_records_from_list(records: List[Dict[str, Any]], config: Dict[str, Any], logger: CustomLogger) -> List[Dict[str, Any]]:
    stale_records = []
    timezone = config.get("timezone", "UTC")
    tz = pytz.timezone(timezone)
    now = datetime.now(tz)
    stale_factor = config.get("stale_threshold_factor", 3)

    for i, record in enumerate(records, 1):
        try:
            print(f"\n[Record #{i}] Checking staleness...")

            duration_str = record.get("PIPELINE_EXP_DURATION") or config["PIPELINE_EXP_DURATION"]
            expected_secs = parse_duration_to_seconds(duration_str)
            start_time = datetime.fromisoformat(record["PIPELINE_START_TIME"])
            actual_secs = (now - start_time).total_seconds()

            print(f"  Expected: {expected_secs}s | Actual: {actual_secs}s | Factor: {stale_factor}")
            if actual_secs > expected_secs * stale_factor:
                print("  --> Marked as STALE")
                stale_records.append(record)
            else:
                print("  --> Still in progress")

        except Exception as e:
            logger.warning(
                f"Error parsing record for staleness: {str(e)}",
                keyword="STALE_PARSE_ERROR",
                other_details={"record_index": i}
            )
            continue

    return stale_records


def parse_duration_to_seconds(duration_str: str) -> int:
    print(f"Parsing duration string: {duration_str}")
    total = 0

    matchers = {
        'd': 86400,
        'h': 3600,
        'm': 60,
        's': 1
    }

    for unit, factor in matchers.items():
        match = re.search(fr"(\d+){unit}", duration_str)
        if match:
            value = int(match.group(1))
            total += value * factor
            print(f"  Found {value}{unit} = {value * factor} seconds")

    if total == 0:
        raise ValueError(f"No valid time unit found in: '{duration_str}'")

    return total


def mark_record_as_pending(record: Dict[str, Any]) -> None:
    """
    Resets the appropriate parts of a stale record to prepare it for reprocessing.
    Skips any phase that is already marked as 'COMPLETED'.
    """
    print(f"\nMarking PIPELINE_ID={record.get('PIPELINE_ID')} as PENDING...")

    # Always reset top-level pipeline status
    print("Resetting pipeline-level status and timing...")
    record['PIPELINE_STATUS'] = 'PENDING'
    record['PIPELINE_START_TIME'] = None
    record['PIPELINE_END_TIME'] = None
    record['PIPELINE_DURATION'] = None

    # Define all phases and their associated fields
    phase_keys = [
        ('SRC_STG_XFER_STATUS', 'SRC_STG_XFER_START_TS', 'SRC_STG_XFER_END_TS', 'SRC_STG_XFER_DURATION'),
        ('SRC_STG_AUDIT_STATUS', 'SRC_STG_AUDIT_START_TS', 'SRC_STG_AUDIT_END_TS', 'SRC_STG_AUDIT_DURATION'),
        ('STG_TGT_XFER_STATUS', 'STG_TGT_XFER_START_TS', 'STG_TGT_XFER_END_TS', 'STG_TGT_XFER_DURATION'),
        ('STG_TGT_AUDIT_STATUS', 'STG_TGT_AUDIT_START_TS', 'STG_TGT_AUDIT_END_TS', 'STG_TGT_AUDIT_DURATION'),
        ('SRC_TGT_AUDIT_STATUS', 'SRC_TGT_AUDIT_START_TS', 'SRC_TGT_AUDIT_END_TS', 'SRC_TGT_AUDIT_DURATION')
    ]

    for status_key, start_ts_key, end_ts_key, duration_key in phase_keys:
        current_status = record.get(status_key)
        print(f"Checking phase: {status_key} = {current_status}")

        if current_status == 'COMPLETED':
            print(f"  Skipping {status_key} — already completed.")
            continue

        print(f"  Resetting {status_key} to 'PENDING' and clearing timestamps/duration...")
        record[status_key] = 'PENDING'
        record[start_ts_key] = None
        record[end_ts_key] = None
        record[duration_key] = None

    # Increment retry attempt number safely
    record['RETRY_ATTEMPT_NUMBER'] = (record.get('RETRY_ATTEMPT_NUMBER') or 0) + 1
    print(f"Updated RETRY_ATTEMPT_NUMBER: {record['RETRY_ATTEMPT_NUMBER']}")


def convert_all_stale_to_pending(stale_records: List[Dict[str, Any]],
                                  original_records: List[Dict[str, Any]],
                                  config: Dict[str, Any],
                                  logger: CustomLogger) -> int:
    converted = 0

    for i, record in enumerate(stale_records):
        try:
            print(f"\nConverting stale record {i + 1}/{len(stale_records)}...")
            mark_record_as_pending(record)
            delete_old_in_process_record_and_insert_new_pending_record(original_records[i], record, config, logger)
            print(" Conversion success.")
            converted += 1

        except Exception as e:
            logger.error(
                f"Failed to convert stale record: {str(e)}",
                keyword="CONVERT_TO_PENDING_FAILED",
                other_details={"record_index": i}
            )
            print(" Conversion failed.")

    return converted




