"""
Robot Telemetry Data Collection Module

This module provides a simple interface for robots to log telemetry data.
It handles data collection, metadata enrichment, and submission to the local buffer.
"""

import json
import os
import time
import uuid
import socket
import logging
import platform
from datetime import datetime
from typing import Dict, Any, Optional, List, Union

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("telemetry-data-collection")

class TelemetryData:
    """Class to collect and format telemetry data from robots"""
    
    def __init__(self, robot_id: str):
        """
        Initialize the telemetry data collector
        
        Args:
            robot_id: Unique identifier for the robot
        """
        self.robot_id = robot_id
        self.device_id = str(uuid.uuid4())
        self.hostname = socket.gethostname()
        self.start_time = datetime.now().isoformat()
        self.session_id = str(uuid.uuid4())
        
        # System info
        self.system_info = {
            "os": platform.system(),
            "platform": platform.platform(),
            "python_version": platform.python_version(),
        }
        
        # Load or create device info file
        self.device_info_path = f".device_info_{robot_id}.json"
        self.device_info = self._load_device_info()
        
        logger.info(f"TelemetryData initialized for robot {robot_id}")
    
    def _load_device_info(self) -> Dict[str, Any]:
        """Load device info from file or create if not exists"""
        if os.path.exists(self.device_info_path):
            try:
                with open(self.device_info_path, 'r') as f:
                    return json.load(f)
            except (json.JSONDecodeError, IOError) as e:
                logger.warning(f"Failed to load device info: {e}")
        
        # Create new device info
        device_info = {
            "robot_id": self.robot_id,
            "device_id": self.device_id,
            "first_seen": self.start_time,
            "hostname": self.hostname
        }
        
        # Save to file
        try:
            with open(self.device_info_path, 'w') as f:
                json.dump(device_info, f)
        except IOError as e:
            logger.warning(f"Failed to save device info: {e}")
        
        return device_info
    
    def collect(self, 
                data: Dict[str, Any], 
                data_type: str = "telemetry",
                tags: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Collect and format telemetry data
        
        Args:
            data: The actual telemetry data dictionary
            data_type: Type of data (telemetry, event, log, etc.)
            tags: Optional list of tags to categorize the data
            
        Returns:
            Formatted telemetry data dictionary
        """
        timestamp = datetime.now().isoformat()
        
        # Format telemetry data with metadata
        telemetry_data = {
            "robot_id": self.robot_id,
            "device_id": self.device_id,
            "session_id": self.session_id,
            "timestamp": timestamp,
            "data_type": data_type,
            "data": data
        }
        
        # Add optional tags
        if tags:
            telemetry_data["tags"] = tags
        
        return telemetry_data
    
    def log_event(self, 
                 event_name: str, 
                 event_data: Optional[Dict[str, Any]] = None, 
                 severity: str = "INFO", 
                 tags: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Log a specific event
        
        Args:
            event_name: Name of the event
            event_data: Additional event data
            severity: Event severity (INFO, WARN, ERROR, etc.)
            tags: Optional list of tags
            
        Returns:
            Formatted event data
        """
        event = {
            "event_name": event_name,
            "severity": severity,
            "timestamp_ms": int(time.time() * 1000)
        }
        
        if event_data:
            event["event_data"] = event_data
        
        return self.collect(event, data_type="event", tags=tags)
    
    def log_error(self, 
                 error_message: str, 
                 error_type: Optional[str] = None,
                 stack_trace: Optional[str] = None,
                 context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Log an error
        
        Args:
            error_message: Error message
            error_type: Type of error
            stack_trace: Stack trace if available
            context: Contextual information about when the error occurred
            
        Returns:
            Formatted error data
        """
        error_data = {
            "error_message": error_message,
            "timestamp_ms": int(time.time() * 1000)
        }
        
        if error_type:
            error_data["error_type"] = error_type
            
        if stack_trace:
            error_data["stack_trace"] = stack_trace
            
        if context:
            error_data["context"] = context
        
        return self.collect(error_data, data_type="error", tags=["error"])
    
    def log_metrics(self,
                   metrics: Dict[str, Union[float, int]],
                   metric_group: Optional[str] = None) -> Dict[str, Any]:
        """
        Log numerical metrics
        
        Args:
            metrics: Dictionary of metric names and values
            metric_group: Optional grouping for the metrics
            
        Returns:
            Formatted metrics data
        """
        metrics_data = {
            "metrics": metrics,
            "timestamp_ms": int(time.time() * 1000)
        }
        
        if metric_group:
            metrics_data["metric_group"] = metric_group
            
        tags = ["metrics"]
        if metric_group:
            tags.append(metric_group)
            
        return self.collect(metrics_data, data_type="metrics", tags=tags)


# Example usage
if __name__ == "__main__":
    # Create a telemetry data collector
    telemetry = TelemetryData("robot-001")
    
    # Collect telemetry data
    sensor_data = {
        "temperature": 24.5,
        "humidity": 65.2,
        "pressure": 1013.2,
        "battery": 87.5,
        "position": {"x": 10.5, "y": 20.3, "z": 0.0},
        "orientation": {"roll": 0.1, "pitch": 0.2, "yaw": 0.3}
    }
    
    telemetry_data = telemetry.collect(sensor_data)
    print(json.dumps(telemetry_data, indent=2))
    
    # Log an event
    event_data = telemetry.log_event("navigation_started", 
                                    {"destination": {"x": 100, "y": 200}})
    print(json.dumps(event_data, indent=2))
    
    # Log an error
    error_data = telemetry.log_error("Failed to connect to sensor", 
                                    error_type="ConnectionError",
                                    context={"sensor_id": "IMU-01"})
    print(json.dumps(error_data, indent=2))
    
    # Log metrics
    metrics_data = telemetry.log_metrics({
        "cpu_usage": 45.2,
        "memory_usage": 512.7,
        "disk_usage": 1024.3
    }, metric_group="system")
    print(json.dumps(metrics_data, indent=2))