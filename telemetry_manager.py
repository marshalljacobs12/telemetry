"""
Robot Telemetry Manager

This is the main module that ties together data collection, local buffering,
and connectivity management for the robot telemetry system.
"""

import json
import os
import time
import logging
import threading
import signal
from datetime import datetime
from typing import Dict, Any, List, Optional, Callable, Union

# Import our components
from data_collection import TelemetryData
from local_buffer import LocalBuffer
from connectivity_manager import ConnectivityManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("telemetry-manager")

class TelemetryManager:
    """Main class that coordinates the telemetry system"""
    
    def __init__(self, robot_id: str, config_path: str = "config.json"):
        """
        Initialize the telemetry manager
        
        Args:
            robot_id: Unique identifier for this robot
            config_path: Path to configuration file
        """
        self.robot_id = robot_id
        
        # Load configuration
        self.config = self._load_config(config_path)
        
        # Create data directories if they don't exist
        data_dir = self.config.get("data_dir", "data")
        log_dir = self.config.get("log_dir", "logs")
        
        os.makedirs(data_dir, exist_ok=True)
        os.makedirs(log_dir, exist_ok=True)
        
        # Configure logging level
        log_level = getattr(logging, self.config.get("log_level", "INFO"))
        logging.getLogger().setLevel(log_level)
        
        # Initialize components
        self.data_collector = TelemetryData(robot_id)
        
        # Set up local buffer
        db_path = os.path.join(data_dir, f"telemetry_{robot_id}.db")
        self.local_buffer = LocalBuffer(
            robot_id,
            db_path=db_path,
            compression_enabled=self.config.get("compression_enabled", True),
            max_db_size_mb=self.config.get("max_database_size_mb", 1000),
            retention_days=self.config.get("retention_period_days", 30)
        )
        
        # Set up connectivity manager
        self.connectivity = ConnectivityManager(
            robot_id,
            self.config,
            on_status_change=self._on_connectivity_change
        )
        
        # Background worker for sending buffered data
        self.running = False
        self.buffer_sender_thread = None
        self.maintenance_thread = None
        self.stats = {}
        
        logger.info(f"Telemetry manager initialized for robot {robot_id}")
    
    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Load configuration from file"""
        try:
            with open(config_path, 'r') as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError) as e:
            logger.warning(f"Error loading config, using defaults: {e}")
            
            # Default configuration
            return {
                "aws_region": "us-east-1",
                "iot_endpoint": None,
                "s3_bucket": "robot-telemetry-backup",
                "connection_check_interval": 60,
                "batch_size": 100,
                "log_level": "INFO",
                "retention_period_days": 30,
                "max_database_size_mb": 1000,
                "compression_enabled": True,
                "data_dir": "data",
                "log_dir": "logs"
            }
    
    def start(self):
        """Start the telemetry manager"""
        if self.running:
            logger.warning("Telemetry manager already running")
            return
        
        self.running = True
        
        # Start connectivity manager
        self.connectivity.start()
        
        # Start buffer sender thread
        self.buffer_sender_thread = threading.Thread(
            target=self._buffer_sender_worker,
            name="buffer-sender"
        )
        self.buffer_sender_thread.daemon = True
        self.buffer_sender_thread.start()
        
        # Start maintenance thread
        self.maintenance_thread = threading.Thread(
            target=self._maintenance_worker,
            name="maintenance-worker"
        )
        self.maintenance_thread.daemon = True
        self.maintenance_thread.start()
        
        logger.info("Telemetry manager started")
    
    def stop(self):
        """Stop the telemetry manager"""
        if not self.running:
            return
        
        self.running = False
        
        # Stop connectivity manager
        self.connectivity.stop()
        
        # Wait for threads to finish
        if self.buffer_sender_thread:
            self.buffer_sender_thread.join(timeout=5)
        
        if self.maintenance_thread:
            self.maintenance_thread.join(timeout=5)
        
        # Final maintenance
        self.local_buffer.clean_old_data()
        
        logger.info("Telemetry manager stopped")
    
    def _on_connectivity_change(self, status: str, details: Any):
        """Callback when connectivity status changes"""
        logger.info(f"Connectivity changed: {status}")
        
        # If we just connected, trigger immediate buffer send
        if status == "connected":
            self._trigger_buffer_send()
    
    '''
    worker thread
    '''
    def _buffer_sender_worker(self):
        """Worker thread that sends buffered data when connected"""
        while self.running:
            try:
                # Check if we're connected
                if self.connectivity.connected:
                    # Get a batch of unsent data
                    batch_size = self.config.get("batch_size", 100)
                    unsent_batch = self.local_buffer.get_unsent(limit=batch_size)
                    
                    if unsent_batch:
                        logger.info(f"Sending {len(unsent_batch)} buffered records")
                        
                        # Send each record
                        successful_ids = []
                        failed_ids = []
                        
                        for entry in unsent_batch:
                            # Publish to IoT Core
                            success = self.connectivity.publish(
                                entry['data'],
                                topic=f"telemetry/{self.robot_id}/{entry['data_type']}" if 'data_type' in entry['data'] else None,
                                qos=1
                            )
                            
                            if success:
                                successful_ids.append(entry['id'])
                            else:
                                failed_ids.append(entry['id'])
                        
                        # Update local buffer
                        if successful_ids:
                            self.local_buffer.mark_as_sent(successful_ids)
                            logger.info(f"Marked {len(successful_ids)} records as sent")
                        
                        if failed_ids:
                            self.local_buffer.update_retry(failed_ids)
                            logger.warning(f"Failed to send {len(failed_ids)} records")
                
                # Sleep before next batch
                # Use shorter interval if we're connected and have unsent data
                if self.connectivity.connected and unsent_batch:
                    time.sleep(1)  # Send continuously when connected
                else:
                    time.sleep(10)  # Wait longer when disconnected or buffer empty
            
            except Exception as e:
                logger.error(f"Error in buffer sender: {e}")
                time.sleep(30)  # Longer sleep on error
    
    '''
    creates new thread and calls _process_buffer_batch instead of using worker thread
    '''
    def _trigger_buffer_send(self):
        """Trigger immediate sending of buffered data"""
        if self.buffer_sender_thread and self.buffer_sender_thread.is_alive():
            # We can't directly trigger the thread, but we can create a new one
            # for immediate processing
            send_thread = threading.Thread(
                target=self._process_buffer_batch,
                name="immediate-buffer-send"
            )
            send_thread.daemon = True
            send_thread.start()
    
    '''
    similar to _buffer_sender_worker just not on worker thread
    '''
    def _process_buffer_batch(self):
        """Process a single batch of buffered data"""
        try:
            if not self.connectivity.connected:
                logger.debug("Not connected, skipping buffer processing")
                return
            
            # Get a batch of unsent data
            batch_size = self.config.get("batch_size", 100)
            unsent_batch = self.local_buffer.get_unsent(limit=batch_size)
            
            if unsent_batch:
                logger.info(f"Processing {len(unsent_batch)} buffered records")
                
                # Send each record
                successful_ids = []
                failed_ids = []
                
                for entry in unsent_batch:
                    # Determine topic based on data type
                    data_type = entry['data'].get('data_type', 'telemetry')
                    topic = f"telemetry/{self.robot_id}/{data_type}"
                    
                    # Publish to IoT Core
                    success = self.connectivity.publish(
                        entry['data'],
                        topic=topic,
                        qos=1
                    )
                    
                    if success:
                        successful_ids.append(entry['id'])
                    else:
                        failed_ids.append(entry['id'])
                
                # Update local buffer
                if successful_ids:
                    self.local_buffer.mark_as_sent(successful_ids)
                    logger.info(f"Marked {len(successful_ids)} records as sent")
                
                if failed_ids:
                    self.local_buffer.update_retry(failed_ids)
                    logger.warning(f"Failed to send {len(failed_ids)} records")
        
        except Exception as e:
            logger.error(f"Error processing buffer batch: {e}")
    
    def _maintenance_worker(self):
        """Worker thread for system maintenance"""
        # Calculate maintenance interval (in seconds)
        maintenance_interval = self.config.get("maintenance_interval_hours", 24) * 3600
        
        # Initial delay to avoid maintenance at startup
        time.sleep(60)
        
        while self.running:
            try:
                # Run maintenance tasks
                logger.info("Running scheduled maintenance")
                
                # Clean up old data
                deleted = self.local_buffer.clean_old_data()
                if deleted:
                    logger.info(f"Cleaned up {deleted} old records")
                
                # Optimize database
                self.local_buffer.optimize()
                
                # Update statistics
                self.update_stats()
                
                # Sleep until next maintenance
                for _ in range(maintenance_interval):
                    if not self.running:
                        break
                    time.sleep(1)
            
            except Exception as e:
                logger.error(f"Error in maintenance worker: {e}")
                time.sleep(3600)  # Sleep for an hour on error
    
    def log_telemetry(self, data: Dict[str, Any], tags: Optional[List[str]] = None) -> bool:
        """
        Log telemetry data
        
        Args:
            data: The telemetry data to log
            tags: Optional list of tags
            
        Returns:
            Whether the operation was successful
        """
        try:
            # Use data collector to format the data
            telemetry_data = self.data_collector.collect(data, data_type="telemetry", tags=tags)
            
            # Store in local buffer
            self.local_buffer.store(telemetry_data)
            
            # If connected, publish immediately
            if self.connectivity.connected:
                self.connectivity.publish(
                    telemetry_data,
                    topic=f"telemetry/{self.robot_id}/telemetry",
                    qos=1
                )
            
            return True
        
        except Exception as e:
            logger.error(f"Error logging telemetry: {e}")
            return False
    
    def log_event(self, 
                 event_name: str, 
                 event_data: Optional[Dict[str, Any]] = None, 
                 severity: str = "INFO", 
                 tags: Optional[List[str]] = None) -> bool:
        """
        Log an event
        
        Args:
            event_name: Name of the event
            event_data: Additional event data
            severity: Event severity
            tags: Optional list of tags
            
        Returns:
            Whether the operation was successful
        """
        try:
            # Use data collector to format the event
            event = self.data_collector.log_event(event_name, event_data, severity, tags)
            
            # Set priority based on severity (higher for more severe events)
            priority = 1
            if severity == "WARN":
                priority = 2
            elif severity == "ERROR":
                priority = 3
            elif severity == "CRITICAL":
                priority = 4
            
            # Store in local buffer with appropriate priority
            self.local_buffer.store(event, priority=priority)
            
            # If connected, publish immediately (especially for high priority events)
            if self.connectivity.connected or priority >= 3:
                self.connectivity.publish(
                    event,
                    topic=f"telemetry/{self.robot_id}/event",
                    qos=1
                )
            
            return True
        
        except Exception as e:
            logger.error(f"Error logging event: {e}")
            return False
    
    def log_error(self, 
                 error_message: str, 
                 error_type: Optional[str] = None,
                 stack_trace: Optional[str] = None,
                 context: Optional[Dict[str, Any]] = None) -> bool:
        """
        Log an error
        
        Args:
            error_message: Error message
            error_type: Type of error
            stack_trace: Stack trace if available
            context: Contextual information
            
        Returns:
            Whether the operation was successful
        """
        try:
            # Use data collector to format the error
            error_data = self.data_collector.log_error(error_message, error_type, stack_trace, context)
            
            # Store in local buffer with high priority
            self.local_buffer.store(error_data, priority=4)
            
            # If connected, publish immediately (errors should be prioritized)
            if self.connectivity.connected:
                self.connectivity.publish(
                    error_data,
                    topic=f"telemetry/{self.robot_id}/error",
                    qos=1
                )
            
            return True
        
        except Exception as e:
            logger.error(f"Error logging error: {e}")
            return False
    
    def log_metrics(self,
                   metrics: Dict[str, Union[float, int]],
                   metric_group: Optional[str] = None) -> bool:
        """
        Log numerical metrics
        
        Args:
            metrics: Dictionary of metric names and values
            metric_group: Optional grouping for the metrics
            
        Returns:
            Whether the operation was successful
        """
        try:
            # Use data collector to format the metrics
            metrics_data = self.data_collector.log_metrics(metrics, metric_group)
            
            # Store in local buffer
            self.local_buffer.store(metrics_data)
            
            # If connected, publish immediately
            if self.connectivity.connected:
                self.connectivity.publish(
                    metrics_data,
                    topic=f"telemetry/{self.robot_id}/metrics",
                    qos=1
                )
            
            return True
        
        except Exception as e:
            logger.error(f"Error logging metrics: {e}")
            return False
    
    def update_stats(self) -> Dict[str, Any]:
        """
        Update and return system statistics
        
        Returns:
            Dictionary of statistics
        """
        try:
            # Get buffer statistics
            buffer_stats = self.local_buffer.get_stats()
            
            # Get connectivity status
            connectivity_status = self.connectivity.get_status()
            
            # Combine stats
            self.stats = {
                "timestamp": datetime.now().isoformat(),
                "robot_id": self.robot_id,
                "buffer": buffer_stats,
                "connectivity": connectivity_status,
                "system": {
                    "uptime": time.time() - self.stats.get("start_time", time.time())
                }
            }
            
            # Set start time if not set
            if "start_time" not in self.stats:
                self.stats["start_time"] = time.time()
            
            return self.stats
        
        except Exception as e:
            logger.error(f"Error updating stats: {e}")
            return {}
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get current system statistics
        
        Returns:
            Dictionary of statistics
        """
        return self.update_stats()


# Example usage as standalone module
if __name__ == "__main__":
    import argparse
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Robot Telemetry Manager')
    parser.add_argument('--robot-id', type=str, default="robot-001", help='Robot ID')
    parser.add_argument('--config', type=str, default="config.json", help='Path to config file')
    parser.add_argument('--simulate', action='store_true', help='Run simulation')
    args = parser.parse_args()
    
    # Create telemetry manager
    manager = TelemetryManager(args.robot_id, args.config)
    
    # Handle graceful shutdown
    def handle_shutdown(sig, frame):
        print("\nShutting down telemetry manager...")
        manager.stop()
        print("Shutdown complete.")
        exit(0)
    
    # Register signal handlers
    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)
    
    # Start manager
    manager.start()
    
    print(f"Telemetry manager started for robot {args.robot_id}")
    print("Press Ctrl+C to stop")
    
    # If simulation is enabled, generate some test data
    if args.simulate:
        import random
        
        print("Running simulation mode")
        
        try:
            iteration = 0
            while True:
                # Log telemetry data
                telemetry = {
                    "temperature": 25 + random.uniform(-5, 5),
                    "battery": max(0, min(100, 80 - iteration * 0.1 + random.uniform(-1, 1))),
                    "position": {
                        "x": 10 + iteration * 0.2 + random.uniform(-0.5, 0.5),
                        "y": 20 + random.uniform(-0.5, 0.5),
                        "z": 0
                    },
                    "velocity": {
                        "x": 0.2 + random.uniform(-0.05, 0.05),
                        "y": random.uniform(-0.05, 0.05),
                        "z": 0
                    },
                    "sensors": {
                        "infrared": [random.uniform(0.5, 4.0) for _ in range(5)],
                        "ultrasonic": random.uniform(0.5, 5.0)
                    }
                }
                
                manager.log_telemetry(telemetry)
                
                # Log metrics
                metrics = {
                    "cpu_usage": 10 + random.uniform(0, 30),
                    "memory_usage": 50 + random.uniform(0, 20),
                    "disk_usage": 20 + iteration * 0.1
                }
                
                manager.log_metrics(metrics, "system")
                
                # Occasionally log events
                if random.random() < 0.1:
                    event_type = random.choice(["navigation", "sensor", "system"])
                    severity = random.choice(["INFO", "WARN", "ERROR"])
                    
                    manager.log_event(
                        f"{event_type}_event",
                        {"detail": f"Simulated {severity} event", "value": random.randint(1, 100)},
                        severity
                    )
                
                # Very rarely log errors
                if random.random() < 0.02:
                    error_types = ["ConnectionError", "TimeoutError", "SensorError"]
                    error_type = random.choice(error_types)
                    
                    manager.log_error(
                        f"Simulated {error_type}",
                        error_type,
                        "Simulated stack trace",
                        {"simulation": True}
                    )
                
                # Print stats every 10 iterations
                if iteration % 10 == 0:
                    stats = manager.get_stats()
                    print(f"\nIteration {iteration}")
                    print(f"Buffer: {stats['buffer'].get('total_entries', 0)} entries, "
                          f"{stats['buffer'].get('unsent_entries', 0)} unsent")
                    print(f"Connectivity: {'Connected' if stats['connectivity'].get('connected') else 'Disconnected'}")
                    
                    # Simulate occasional WiFi issues
                    if iteration > 0 and iteration % 50 == 0:
                        print("\nSimulating connectivity issue...")
                        # Can't actually disconnect, but we can simulate by logging events
                        manager.log_event("connectivity_issue", 
                                         {"type": "wifi_dropout", "duration": "30s"},
                                         "WARN")
                
                iteration += 1
                time.sleep(1)
        
        except KeyboardInterrupt:
            pass
    
    else:
        # If not simulating, just keep the process alive
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            pass
    
    # Stop manager on exit
    manager.stop()