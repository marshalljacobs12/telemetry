"""
Connectivity Manager for Robot Telemetry

This module handles network connectivity checks and data transmission to AWS IoT Core.
It manages the transition between offline and online modes.
"""

import json
import time
import logging
import threading
import random
import queue
import os
import subprocess
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Callable, Union

import boto3
from botocore.exceptions import ClientError
import requests

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("telemetry-connectivity")

class ConnectivityManager:
    """Manages network connectivity and data transmission to AWS"""
    
    def __init__(self, 
                robot_id: str, 
                config: Dict[str, Any],
                on_status_change: Optional[Callable[[str, Any], None]] = None):
        """
        Initialize the connectivity manager
        
        Args:
            robot_id: Unique identifier for the robot
            config: Configuration dictionary
            on_status_change: Optional callback for connectivity status changes
        """
        self.robot_id = robot_id
        self.config = config
        self.on_status_change = on_status_change
        
        # Extract configuration
        self.aws_region = config.get('aws_region', 'us-east-1')
        self.iot_endpoint = config.get('iot_endpoint')
        self.check_interval = config.get('connection_check_interval', 60)
        self.retry_backoff_base = config.get('retry_backoff_base', 5)
        self.max_retries = config.get('max_retries', 5)
        self.wifi_monitor_enabled = config.get('wifi_monitor', {}).get('enabled', True)
        self.wifi_check_interval = config.get('wifi_monitor', {}).get('check_interval', 15)
        self.attempt_reconnect = config.get('wifi_monitor', {}).get('attempt_reconnect', True)
        self.signal_strength_threshold = config.get('robot_specific', {}).get('signal_strength_threshold', -80)
        
        # State tracking
        self.connected = False
        self.consecutive_failures = 0
        self.last_success = None
        self.signal_strength = 0
        self.current_ssid = None
        self.status_log = []  # Track recent status changes
        
        # AWS clients
        self.iot_client = None
        self.s3_client = None
        
        # Transmission queue and lock
        self.transmit_queue = queue.Queue()
        self.queue_lock = threading.Lock()
        
        # Worker threads
        self.running = True
        self.connectivity_thread = None
        self.transmit_thread = None
        self.wifi_monitor_thread = None
        
        # Status tracking
        self._record_status_change("initialized", None)
        
        logger.info(f"Connectivity manager initialized for robot {robot_id}")
    
    def start(self):
        """Start the connectivity manager threads"""
        if self.connectivity_thread and self.connectivity_thread.is_alive():
            logger.warning("Connectivity manager already running")
            return
        
        self.running = True
        
        # Start connectivity check thread
        self.connectivity_thread = threading.Thread(
            target=self._connectivity_worker,
            name="connectivity-checker"
        )
        self.connectivity_thread.daemon = True
        self.connectivity_thread.start()
        
        # Start transmission thread
        self.transmit_thread = threading.Thread(
            target=self._transmit_worker,
            name="transmit-worker"
        )
        self.transmit_thread.daemon = True
        self.transmit_thread.start()
        
        # Start WiFi monitor thread if enabled
        if self.wifi_monitor_enabled:
            self.wifi_monitor_thread = threading.Thread(
                target=self._wifi_monitor_worker,
                name="wifi-monitor"
            )
            self.wifi_monitor_thread.daemon = True
            self.wifi_monitor_thread.start()
        
        logger.info("Connectivity manager started")
    
    def stop(self):
        """Stop the connectivity manager threads"""
        self.running = False
        
        # Wait for threads to finish
        if self.connectivity_thread:
            self.connectivity_thread.join(timeout=5)
        
        if self.transmit_thread:
            self.transmit_thread.join(timeout=5)
        
        if self.wifi_monitor_thread:
            self.wifi_monitor_thread.join(timeout=5)
        
        logger.info("Connectivity manager stopped")
    
    def _record_status_change(self, status: str, details: Any):
        """Record a status change and call the callback if provided"""
        timestamp = datetime.now().isoformat()
        status_entry = {
            "timestamp": timestamp,
            "status": status,
            "details": details
        }
        
        # Add to status log, keeping last 20 entries
        self.status_log.append(status_entry)
        if len(self.status_log) > 20:
            self.status_log.pop(0)
        
        # Call the callback if provided
        if self.on_status_change:
            try:
                self.on_status_change(status, details)
            except Exception as e:
                logger.error(f"Error in status change callback: {e}")
        
        logger.info(f"Connectivity status: {status}")
    
    def _check_connectivity(self) -> bool:
        """
        Check if we have internet connectivity
        
        Returns:
            Whether we have connectivity
        """
        try:
            # First check: Can we reach the AWS IoT endpoint?
            if self.iot_endpoint:
                endpoint_url = f"https://{self.iot_endpoint}"
                response = requests.head(endpoint_url, timeout=5)
                if response.status_code < 400:  # Any success or redirection
                    return True
            
            # Second check: Can we reach a reliable public service?
            response = requests.head("https://aws.amazon.com", timeout=5)
            return response.status_code < 400
        
        except (requests.RequestException, ConnectionError):
            return False
    
    '''
    might be unnecessary
    '''
    def _get_wifi_info(self) -> Dict[str, Any]:
        """
        Get information about current WiFi connection
        
        Returns:
            Dictionary with WiFi information
        """
        result = {
            "connected": False,
            "ssid": None,
            "signal_strength": 0,
            "interface": None
        }
        
        try:
            if os.name == 'nt':  # Windows
                # Use netsh to get WiFi info
                output = subprocess.check_output(
                    ["netsh", "wlan", "show", "interfaces"], 
                    universal_newlines=True
                )
                
                for line in output.split('\n'):
                    if "SSID" in line and "BSSID" not in line:
                        result["ssid"] = line.split(':', 1)[1].strip()
                        result["connected"] = bool(result["ssid"])
                    elif "Signal" in line:
                        signal_str = line.split(':', 1)[1].strip().rstrip('%')
                        try:
                            # Convert percentage to dBm (very approximate)
                            percent = int(signal_str)
                            # Map 0-100% to -100 to -30 dBm
                            result["signal_strength"] = -100 + percent * 0.7
                        except ValueError:
                            pass
                    elif "Name" in line:
                        result["interface"] = line.split(':', 1)[1].strip()
            
            else:  # Unix-like (Linux, macOS)
                # Try different commands based on what's available
                try:
                    # Try iwconfig first (Linux)
                    output = subprocess.check_output(
                        ["iwconfig"], 
                        universal_newlines=True,
                        stderr=subprocess.DEVNULL
                    )
                    
                    interface = None
                    for line in output.split('\n'):
                        if "ESSID" in line:
                            interface = line.split(' ', 1)[0]
                            essid = line.split('ESSID:"', 1)[1].split('"', 1)[0]
                            if essid:
                                result["ssid"] = essid
                                result["connected"] = True
                                result["interface"] = interface
                        
                        if interface and "Signal level" in line:
                            signal_part = line.split('Signal level=', 1)[1].split(' ', 1)[0]
                            try:
                                if 'dBm' in signal_part:
                                    result["signal_strength"] = float(signal_part.rstrip('dBm'))
                                else:
                                    # Convert to dBm if needed
                                    result["signal_strength"] = float(signal_part)
                            except ValueError:
                                pass
                
                except (subprocess.SubprocessError, FileNotFoundError):
                    try:
                        # Try airport command (macOS)
                        output = subprocess.check_output(
                            ["/System/Library/PrivateFrameworks/Apple80211.framework/Versions/Current/Resources/airport", "-I"],
                            universal_newlines=True,
                            stderr=subprocess.DEVNULL
                        )
                        
                        for line in output.split('\n'):
                            if "SSID" in line and "BSSID" not in line:
                                result["ssid"] = line.split(':', 1)[1].strip()
                                result["connected"] = bool(result["ssid"])
                            elif "agrCtlRSSI" in line:
                                try:
                                    result["signal_strength"] = float(line.split(':', 1)[1].strip())
                                except ValueError:
                                    pass
                    
                    except (subprocess.SubprocessError, FileNotFoundError):
                        # Fall back to generic check
                        if self._check_connectivity():
                            result["connected"] = True
                            result["signal_strength"] = -60  # Assume moderate signal
        
        except Exception as e:
            logger.warning(f"Error getting WiFi info: {e}")
        
        return result
    
    def _attempt_wifi_reconnect(self) -> bool:
        """
        Attempt to reconnect to WiFi if disconnected
        
        Returns:
            Whether reconnection was successful
        """
        if not self.attempt_reconnect:
            return False
        
        try:
            if os.name == 'nt':  # Windows
                # On Windows, use netsh to reconnect
                subprocess.run(
                    ["netsh", "wlan", "connect"],
                    check=True,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL
                )
            else:  # Unix-like
                # On Linux, try nmcli (Network Manager)
                try:
                    subprocess.run(
                        ["nmcli", "radio", "wifi", "off"],
                        check=True,
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL
                    )
                    time.sleep(1)
                    subprocess.run(
                        ["nmcli", "radio", "wifi", "on"],
                        check=True,
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL
                    )
                except (subprocess.SubprocessError, FileNotFoundError):
                    # Try other methods or log that we can't reconnect
                    logger.warning("WiFi reconnection not supported on this platform")
                    return False
            
            # Wait a bit for reconnection
            time.sleep(5)
            
            # Check if we're reconnected
            wifi_info = self._get_wifi_info()
            return wifi_info["connected"]
        
        except Exception as e:
            logger.error(f"Error attempting WiFi reconnection: {e}")
            return False
    
    def _connectivity_worker(self):
        """Worker thread to check connectivity periodically"""
        while self.running:
            try:
                # Check connectivity
                has_connection = self._check_connectivity()
                
                if has_connection:
                    if not self.connected:
                        # We just regained connectivity
                        self._record_status_change("connected", {"consecutive_failures": self.consecutive_failures})
                    
                    self.connected = True
                    self.consecutive_failures = 0
                    self.last_success = datetime.now()
                else:
                    if self.connected:
                        # We just lost connectivity
                        self._record_status_change("disconnected", {
                            "last_success": self.last_success.isoformat() if self.last_success else None
                        })
                    
                    self.connected = False
                    self.consecutive_failures += 1
                    
                    # Try to reconnect if we've had multiple failures
                    if self.consecutive_failures >= 3 and self.attempt_reconnect:
                        self._record_status_change("reconnecting", {"attempt": self.consecutive_failures - 2})
                        reconnected = self._attempt_wifi_reconnect()
                        if reconnected:
                            self._record_status_change("reconnected", {"attempt": self.consecutive_failures - 2})
                
                # Calculate sleep time (use exponential backoff if disconnected)
                if self.connected:
                    sleep_time = self.check_interval
                else:
                    # Calculate backoff with jitter
                    backoff = min(
                        self.retry_backoff_base * (2 ** min(self.consecutive_failures, 8)),
                        300  # Max 5 minutes
                    )
                    jitter = random.uniform(0.8, 1.2)  # ±20% jitter
                    sleep_time = backoff * jitter
                
                # Sleep until next check
                for _ in range(int(sleep_time)):
                    if not self.running:
                        break
                    time.sleep(1)
            
            except Exception as e:
                logger.error(f"Error in connectivity checker: {e}")
                time.sleep(self.check_interval)  # Sleep and try again
    
    def _wifi_monitor_worker(self):
        """Worker thread to monitor WiFi signal strength and status"""
        if not self.wifi_monitor_enabled:
            return
        
        while self.running:
            try:
                # Get WiFi information
                wifi_info = self._get_wifi_info()
                
                # Update state
                self.signal_strength = wifi_info.get("signal_strength", 0)
                new_ssid = wifi_info.get("ssid")
                
                # Detect SSID change
                if new_ssid != self.current_ssid:
                    if new_ssid:
                        self._record_status_change("wifi_connected", {
                            "ssid": new_ssid,
                            "signal_strength": self.signal_strength
                        })
                    else:
                        self._record_status_change("wifi_disconnected", {
                            "previous_ssid": self.current_ssid
                        })
                    
                    self.current_ssid = new_ssid
                
                # Check signal strength
                if self.signal_strength < self.signal_strength_threshold:
                    # Signal is weak
                    logger.warning(f"Weak WiFi signal: {self.signal_strength} dBm")
                    
                    # If very weak, attempt reconnection
                    if self.signal_strength < self.signal_strength_threshold - 10:
                        self._record_status_change("weak_signal", {
                            "signal_strength": self.signal_strength,
                            "threshold": self.signal_strength_threshold
                        })
                        
                        if self.attempt_reconnect:
                            self._attempt_wifi_reconnect()
                
                # Sleep until next check
                time.sleep(self.wifi_check_interval)
            
            except Exception as e:
                logger.error(f"Error in WiFi monitor: {e}")
                time.sleep(60)  # Sleep and try again
    
    def _init_aws_clients(self):
        """Initialize AWS clients if needed"""
        if not self.iot_client:
            try:
                self.iot_client = boto3.client('iot-data', region_name=self.aws_region)
                logger.info(f"Initialized AWS IoT client for region {self.aws_region}")
            except Exception as e:
                logger.error(f"Failed to initialize AWS IoT client: {e}")
        
        if not self.s3_client:
            try:
                self.s3_client = boto3.client('s3', region_name=self.aws_region)
                logger.info(f"Initialized AWS S3 client for region {self.aws_region}")
            except Exception as e:
                logger.error(f"Failed to initialize AWS S3 client: {e}")
    
    def _transmit_worker(self):
        """Worker thread to transmit queued messages when connected"""
        while self.running:
            try:
                # Only process if connected
                if self.connected:
                    # Initialize AWS clients if needed
                    self._init_aws_clients()
                    
                    # Process any items in the queue
                    processed = 0
                    max_batch = 10  # Process up to 10 items at a time
                    
                    while not self.transmit_queue.empty() and processed < max_batch:
                        try:
                            # Get next item from queue
                            # Don't block if queue is empty
                            item = self.transmit_queue.get(block=False)
                            
                            # Extract data and callback
                            data, topic, qos, callback = item
                            
                            # Send to AWS IoT
                            success = self._publish_to_iot(data, topic, qos)
                            
                            # Call callback with result
                            if callback:
                                try:
                                    callback(success, data)
                                except Exception as e:
                                    logger.error(f"Error in transmit callback: {e}")
                            
                            # Mark as done
                            self.transmit_queue.task_done()
                            processed += 1
                        
                        except queue.Empty:
                            break
                    
                    if processed > 0:
                        logger.debug(f"Processed {processed} queued messages")
                
                # Sleep briefly
                time.sleep(1)
            
            except Exception as e:
                logger.error(f"Error in transmit worker: {e}")
                time.sleep(5)  # Sleep and try again
    
    def _publish_to_iot(self, data: Dict[str, Any], topic: Optional[str] = None, qos: int = 1) -> bool:
        """
        Publish data to AWS IoT Core
        
        Args:
            data: Data to publish
            topic: IoT topic (defaults to telemetry/{robot_id})
            qos: Quality of Service (0 = at most once, 1 = at least once)
            
        Returns:
            Whether the publish was successful
        """
        if not self.iot_client:
            return False
        
        # Use default topic if not specified
        if not topic:
            topic = f"telemetry/{self.robot_id}"
        
        try:
            # Convert data to JSON
            payload = json.dumps(data)
            
            # Publish to IoT Core
            response = self.iot_client.publish(
                topic=topic,
                qos=qos,
                payload=payload
            )
            
            # Check response
            return 'messageId' in response
        
        except ClientError as e:
            logger.error(f"AWS IoT publish error: {e}")
            return False
        
        except Exception as e:
            logger.error(f"Unexpected error publishing to IoT: {e}")
            return False
    
    def publish(self, 
               data: Dict[str, Any], 
               topic: Optional[str] = None,
               qos: int = 1,
               callback: Optional[Callable[[bool, Dict[str, Any]], None]] = None) -> bool:
        """
        Queue data for publication to AWS IoT
        
        Args:
            data: Data to publish
            topic: IoT topic (defaults to telemetry/{robot_id})
            qos: Quality of Service (0 = at most once, 1 = at least once)
            callback: Optional callback when message is sent (or failed)
            
        Returns:
            Whether the message was queued successfully
        """
        try:
            # Add to transmission queue
            self.transmit_queue.put((data, topic, qos, callback))
            return True
        
        except Exception as e:
            logger.error(f"Error queueing message: {e}")
            return False
    
    def get_status(self) -> Dict[str, Any]:
        """
        Get current connectivity status
        
        Returns:
            Dictionary with status information
        """
        status = {
            "connected": self.connected,
            "consecutive_failures": self.consecutive_failures,
            "last_success": self.last_success.isoformat() if self.last_success else None,
            "wifi": {
                "ssid": self.current_ssid,
                "signal_strength": self.signal_strength,
                "signal_quality": "good" if self.signal_strength > -65 else
                               "fair" if self.signal_strength > self.signal_strength_threshold else
                               "poor"
            },
            "queue_size": self.transmit_queue.qsize(),
            "recent_status": self.status_log[-5:] if self.status_log else []
        }
        
        return status


# Example usage
if __name__ == "__main__":
    # Sample configuration
    config = {
        "aws_region": "us-east-1",
        "iot_endpoint": "your-url.us-east-1.amazonaws.com",
        "connection_check_interval": 30,
        "retry_backoff_base": 5,
        "wifi_monitor": {
            "enabled": True,
            "check_interval": 15,
            "attempt_reconnect": True
        },
        "robot_specific": {
            "signal_strength_threshold": -80
        }
    }
    
    # Status change callback
    def status_callback(status, details):
        print(f"Connectivity status changed: {status}")
        if details:
            print(f"Details: {details}")
    
    # Create connectivity manager
    manager = ConnectivityManager("robot-001", config, status_callback)
    
    # Start manager
    manager.start()
    
    try:
        # Simulate some data transmissions
        for i in range(5):
            test_data = {
                "robot_id": "robot-001",
                "timestamp": datetime.now().isoformat(),
                "data_type": "telemetry",
                "data": {
                    "temperature": 24.5 + i,
                    "battery": 90 - i,
                    "position": {"x": i * 10, "y": i * 5, "z": 0}
                }
            }
            
            # Publish callback
            def on_publish(success, data):
                print(f"Message {'sent' if success else 'failed'}: {data.get('data_type')}")
            
            manager.publish(test_data, callback=on_publish)
            time.sleep(5)
        
        # Show current status
        status = manager.get_status()
        print("\nCurrent Status:")
        print(json.dumps(status, indent=2))
        
        # Keep running for testing
        print("\nRunning... (Press Ctrl+C to stop)")
        while True:
            time.sleep(1)
    
    except KeyboardInterrupt:
        print("\nStopping...")
    
    finally:
        # Stop manager
        manager.stop()