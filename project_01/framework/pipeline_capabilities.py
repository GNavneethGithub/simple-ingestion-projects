from typing import Dict, Any, bool
from source_scripts import test_source_connection
from stage_scripts import test_stage_connection  
from target_scripts import test_target_connection
from drive_scripts import test_drive_connection
from custom_logger import CustomLogger
from email_alerts import send_email_alert

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
    
    # Validate required config
    if not config.get('dag_run_id'):
        raise ValueError("Required config field 'dag_run_id' is missing")
    
    try:
        logger.info(
            "Starting connection health check for all pipeline systems",
            keyword="HEALTH_CHECK_START"
        )
    except Exception as e:
        raise Exception(f"Critical: Logger failed during health check start: {e}")
    
    # Test source connection
    try:
        source_status: bool = test_source_connection(config)
    except Exception as e:
        source_status = False
        try:
            logger.warning(
                f"Source connection test crashed: {str(e)}",
                keyword="SOURCE_CONNECTION_CRASH",
                other_details={"error": str(e)}
            )
        except Exception as log_e:
            raise Exception(f"Critical: Logger failed while recording source connection crash: {log_e}")
    
    try:
        logger.info(
            f"Source connection test: {'PASSED' if source_status else 'FAILED'}",
            keyword="SOURCE_CONNECTION_TEST",
            other_details={"status": source_status}
        )
    except Exception as e:
        raise Exception(f"Critical: Logger failed during source connection logging: {e}")
    
    # Test stage connection
    try:
        stage_status: bool = test_stage_connection(config)
    except Exception as e:
        stage_status = False
        try:
            logger.warning(
                f"Stage connection test crashed: {str(e)}",
                keyword="STAGE_CONNECTION_CRASH",
                other_details={"error": str(e)}
            )
        except Exception as log_e:
            raise Exception(f"Critical: Logger failed while recording stage connection crash: {log_e}")
    
    try:
        logger.info(
            f"Stage connection test: {'PASSED' if stage_status else 'FAILED'}",
            keyword="STAGE_CONNECTION_TEST", 
            other_details={"status": stage_status}
        )
    except Exception as e:
        raise Exception(f"Critical: Logger failed during stage connection logging: {e}")
    
    # Test target connection
    try:
        target_status: bool = test_target_connection(config)
    except Exception as e:
        target_status = False
        try:
            logger.warning(
                f"Target connection test crashed: {str(e)}",
                keyword="TARGET_CONNECTION_CRASH",
                other_details={"error": str(e)}
            )
        except Exception as log_e:
            raise Exception(f"Critical: Logger failed while recording target connection crash: {log_e}")
    
    try:
        logger.info(
            f"Target connection test: {'PASSED' if target_status else 'FAILED'}",
            keyword="TARGET_CONNECTION_TEST",
            other_details={"status": target_status}
        )
    except Exception as e:
        raise Exception(f"Critical: Logger failed during target connection logging: {e}")
    
    # Test drive connection
    try:
        drive_status: bool = test_drive_connection(config)
    except Exception as e:
        drive_status = False
        try:
            logger.warning(
                f"Drive connection test crashed: {str(e)}",
                keyword="DRIVE_CONNECTION_CRASH",
                other_details={"error": str(e)}
            )
        except Exception as log_e:
            raise Exception(f"Critical: Logger failed while recording drive connection crash: {log_e}")
    
    try:
        logger.info(
            f"Drive connection test: {'PASSED' if drive_status else 'FAILED'}",
            keyword="DRIVE_CONNECTION_TEST",
            other_details={"status": drive_status}
        )
    except Exception as e:
        raise Exception(f"Critical: Logger failed during drive connection logging: {e}")
    
    result: Dict[str, bool] = {
        'is_source_connection_available': source_status,
        'is_stage_connection_available': stage_status, 
        'is_target_connection_available': target_status,
        'is_drive_connection_available': drive_status
    }
    
    try:
        logger.info(
            "Connection health check completed",
            keyword="HEALTH_CHECK_COMPLETE",
            other_details=result
        )
    except Exception as e:
        raise Exception(f"Critical: Logger failed during health check completion: {e}")
    
    return result

def determine_pipeline_capabilities(config: Dict[str, Any], logger: CustomLogger) -> Dict[str, bool]:
    """
    Determines what pipeline operations can be performed based on connection availability.
    
    Args:
        config: Configuration dictionary containing connection parameters
        logger: CustomLogger instance for logging decisions
    
    Returns:
        Dict with keys: exit_dag, can_process_source_to_stage, can_process_stage_to_target
    """
    
    # Validate required config
    if not config.get('dag_run_id'):
        raise ValueError("Required config field 'dag_run_id' is missing")
    
    # Get connection health status
    connection_status = check_all_connections(config, logger)
    
    # Extract individual connection flags
    drive_available = connection_status.get('is_drive_connection_available', False)
    source_available = connection_status.get('is_source_connection_available', False)
    stage_available = connection_status.get('is_stage_connection_available', False)
    target_available = connection_status.get('is_target_connection_available', False)
    
    # Get DAG run ID for messaging
    dag_run_id = config.get('dag_run_id', 'UNKNOWN')
    
    try:
        logger.info(
            "Starting pipeline capability determination",
            keyword="CAPABILITY_CHECK_START",
            other_details=connection_status
        )
    except Exception as e:
        raise Exception(f"Critical: Logger failed during capability check start: {e}")
    
    # Case 1: Drive connection missing (mandatory) - hard stop
    if not drive_available:
        message = f"Critical: Drive connection unavailable. Cannot log pipeline status. Exiting DAG run {dag_run_id}. All data transfer operations aborted."
        
        try:
            send_email_alert(
                subject=f"CRITICAL: Pipeline Aborted - Drive Connection Missing - DAG {dag_run_id}",
                message=message
            )
        except Exception as e:
            raise Exception(f"Critical: Cannot send alert about drive connection failure: {e}")
        
        try:
            logger.error(
                "Drive connection unavailable - exiting DAG",
                keyword="DRIVE_CONNECTION_FAILED",
                other_details={"dag_run_id": dag_run_id}
            )
        except Exception as e:
            raise Exception(f"Critical: Logger failed while recording drive connection failure: {e}")
        
        return {
            'exit_dag': True,
            'can_process_source_to_stage': False,
            'can_process_stage_to_target': False
        }
    
    # Case 2: All connections missing - hard stop (graceful exit)
    if not any([source_available, stage_available, target_available]):
        message = f"No data connections available (source, stage, target all unavailable). Cannot perform any data transfer operations. Exiting DAG run {dag_run_id}. Will retry in next scheduled run."
        
        try:
            send_email_alert(
                subject=f"WARNING: No Data Connections Available - DAG {dag_run_id}",
                message=message
            )
        except Exception as e:
            raise Exception(f"Critical: Cannot send alert about no data connections: {e}")
        
        try:
            logger.warning(
                "No data connections available - exiting DAG",
                keyword="NO_DATA_CONNECTIONS",
                other_details={"dag_run_id": dag_run_id}
            )
        except Exception as e:
            raise Exception(f"Critical: Logger failed while recording no data connections: {e}")
        
        return {
            'exit_dag': True,
            'can_process_source_to_stage': False,
            'can_process_stage_to_target': False
        }
    
    # Determine processing capabilities
    can_do_source_to_stage = source_available and stage_available
    can_do_stage_to_target = stage_available and target_available
    
    # Generate appropriate alert message based on available operations
    if can_do_source_to_stage and can_do_stage_to_target:
        message = f"All connections available (source, stage, target, drive). Performing complete pipeline: source-to-stage and stage-to-target data transfers. DAG run ID: {dag_run_id}."
        subject = f"INFO: Complete Pipeline Execution - DAG {dag_run_id}"
        
    elif can_do_source_to_stage and not can_do_stage_to_target:
        message = f"Partial pipeline execution: source, stage, and drive connections available. Target connection unavailable. Performing source-to-stage data transfer only. DAG run ID: {dag_run_id}."
        subject = f"WARNING: Partial Pipeline - Source to Stage Only - DAG {dag_run_id}"
        
    elif not can_do_source_to_stage and can_do_stage_to_target:
        message = f"Partial pipeline execution: stage, target, and drive connections available. Source connection unavailable. Performing stage-to-target data transfer only. DAG run ID: {dag_run_id}."
        subject = f"WARNING: Partial Pipeline - Stage to Target Only - DAG {dag_run_id}"
        
    else:
        # Only drive available, no transfers possible
        message = f"Only drive connection available. Source, stage, and target connections unavailable. No data transfer operations possible. DAG run ID: {dag_run_id}. Status will be logged to drive table only."
        subject = f"WARNING: No Data Transfers Possible - DAG {dag_run_id}"
    
    # Send email alert
    try:
        send_email_alert(subject=subject, message=message)
    except Exception as e:
        raise Exception(f"Critical: Cannot send alert about pipeline capabilities: {e}")
    
    # Log the decision
    try:
        logger.info(
            "Pipeline capability determination completed",
            keyword="CAPABILITY_CHECK_COMPLETE",
            other_details={
                "dag_run_id": dag_run_id,
                "can_process_source_to_stage": can_do_source_to_stage,
                "can_process_stage_to_target": can_do_stage_to_target,
                "exit_dag": False
            }
        )
    except Exception as e:
        raise Exception(f"Critical: Logger failed during capability check completion: {e}")
    
    return {
        'exit_dag': False,
        'can_process_source_to_stage': can_do_source_to_stage,
        'can_process_stage_to_target': can_do_stage_to_target
    }





