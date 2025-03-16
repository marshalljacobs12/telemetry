#!/usr/bin/env python3
"""
Example Robot Application with Telemetry Integration

This script demonstrates how to integrate the telemetry system into a robot application.
It simulates a simple robot performing navigation tasks while sending telemetry data.
"""

import os
import sys
import time
import random
import json
import signal
import argparse
import threading
import logging
from datetime import datetime, timedelta
import math

# Add the telemetry system to the path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import telemetry manager
from telemetry_manager import TelemetryManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("example-robot")

class ExampleRobot:
    """Simple simulated robot that demonstrates telemetry integration"""
    
    def __init__(self, robot_id, config_path="config.json"):
        """Initialize the robot"""
        self.robot_id = robot_id
        self.config_path = config_path
        
        # Initialize telemetry
        self.telemetry = TelemetryManager(robot_id, config_path)
        
        # Robot state
        self.state = {
            "position": {"x": 0.0, "y": 0.0, "z": 0.0},
            "orientation": {"roll": 0.0, "pitch": 0.0, "yaw": 0.0},
            "velocity": {"x": 0.0, "y": 0.0, "z": 0.0},
            "battery": 100.0,
            "temperature": 22.0,
            "status": "idle",
            "errors": []
        }
        
        # Navigation target
        self.target = None
        
        # Task queue
        self.tasks = []
        
        # Running flag
        self.running = False
        
        # Threads
        self.navigation_thread = None
        self.sensor_thread = None
        self.battery_thread = None
        
        logger.info(f"Example robot {robot_id} initialized")
    
    def start(self):
        """Start the robot"""
        if self.running:
            logger.warning("Robot already running")
            return
        
        self.running = True
        
        # Start the telemetry system
        self.telemetry.start()
        
        # Start navigation thread
        self.navigation_thread = threading.Thread(
            target=self._navigation_worker,
            name="navigation"
        )
        self.navigation_thread.daemon = True
        self.navigation_thread.start()
        
        # Start sensor thread
        self.sensor_thread = threading.Thread(
            target=self._sensor_worker,
            name="sensors"
        )
        self.sensor_thread.daemon = True
        self.sensor_thread.start()
        
        # Start battery simulation thread
        self.battery_thread = threading.Thread(
            target=self._battery_worker,
            name="battery"
        )
        self.battery_thread.daemon = True
        self.battery_thread.start()
        
        # Log startup event
        self.telemetry.log_event(
            "robot_started",
            {"version": "1.0.0", "mode": "simulation"}
        )
        
        logger.info(f"Robot {self.robot_id} started")
    
    def stop(self):
        """Stop the robot"""
        if not self.running:
            return
        
        self.running = False
        
        # Log shutdown event
        self.telemetry.log_event(
            "robot_stopping",
            {"uptime": time.time() - self.start_time}
        )
        
        # Wait for threads to finish
        if self.navigation_thread:
            self.navigation_thread.join(timeout=5)
        
        if self.sensor_thread:
            self.sensor_thread.join(timeout=5)
        
        if self.battery_thread:
            self.battery_thread.join(timeout=5)
        
        # Stop telemetry system
        self.telemetry.stop()
        
        logger.info(f"Robot {self.robot_id} stopped")
    
    def navigate_to(self, x, y):
        """Set a navigation target"""
        self.target = {"x": x, "y": y}
        self.state["status"] = "navigating"
        
        # Log navigation event
        self.telemetry.log_event(
            "navigation_started",
            {"destination": {"x": x, "y": y}, 
             "current_position": self.state["position"]}
        )
        
        logger.info(f"Navigating to ({x}, {y})")
    
    def add_task(self, task_type, parameters):
        """Add a task to the queue"""
        task = {
            "id": len(self.tasks) + 1,
            "type": task_type,
            "parameters": parameters,
            "status": "queued",
            "created_at": datetime.now().isoformat()
        }
        
        self.tasks.append(task)
        
        # Log task event
        self.telemetry.log_event(
            "task_queued",
            {"task_id": task["id"], "type": task_type}
        )
        
        logger.info(f"Task {task['id']} ({task_type}) added to queue")
    
    def _navigation_worker(self):
        """Worker thread for navigation simulation"""
        self.start_time = time.time()
        
        while self.running:
            try:
                # Update position based on target
                if self.target:
                    # Calculate vector to target
                    dx = self.target["x"] - self.state["position"]["x"]
                    dy = self.target["y"] - self.state["position"]["y"]
                    distance = math.sqrt(dx*dx + dy*dy)
                    
                    if distance < 0.1:
                        # Target reached
                        self.state["velocity"]["x"] = 0.0
                        self.state["velocity"]["y"] = 0.0
                        self.state["status"] = "idle"
                        
                        # Log arrival event
                        self.telemetry.log_event(
                            "navigation_completed",
                            {"destination": self.target, 
                             "final_position": self.state["position"]}
                        )
                        
                        self.target = None
                    else:
                        # Move toward target
                        speed = min(1.0, distance)  # Slow down as we approach
                        
                        # Unit vector in direction of target
                        if distance > 0:
                            dx /= distance
                            dy /= distance
                        
                        # Set velocity
                        self.state["velocity"]["x"] = dx * speed
                        self.state["velocity"]["y"] = dy * speed
                        
                        # Update position
                        self.state["position"]["x"] += self.state["velocity"]["x"] * 0.1
                        self.state["position"]["y"] += self.state["velocity"]["y"] * 0.1
                        
                        # Set yaw to face direction of travel
                        self.state["orientation"]["yaw"] = math.atan2(dy, dx)
                else:
                    # No target, process tasks
                    if self.tasks and self.state["status"] == "idle":
                        task = next((t for t in self.tasks if t["status"] == "queued"), None)
                        if task:
                            task["status"] = "active"
                            
                            # Log task started event
                            self.telemetry.log_event(
                                "task_started",
                                {"task_id": task["id"], "type": task["type"]}
                            )
                            
                            # Process different task types
                            if task["type"] == "navigate":
                                self.navigate_to(
                                    task["parameters"]["x"],
                                    task["parameters"]["y"]
                                )
                            elif task["type"] == "patrol":
                                # Generate patrol points
                                points = task["parameters"]["points"]
                                for point in points:
                                    self.add_task("navigate", {"x": point["x"], "y": point["y"]})
                            
                            # Mark task as completed
                            task["status"] = "completed"
                            task["completed_at"] = datetime.now().isoformat()
                            
                            # Log task completed event
                            self.telemetry.log_event(
                                "task_completed",
                                {"task_id": task["id"], "type": task["type"]}
                            )
                
                # Log telemetry data periodically
                if random.random() < 0.2:  # ~20% chance each iteration = every ~5 seconds
                    # Add some sensor noise
                    noisy_state = {
                        "position": {
                            "x": self.state["position"]["x"] + random.uniform(-0.05, 0.05),
                            "y": self.state["position"]["y"] + random.uniform(-0.05, 0.05),
                            "z": self.state["position"]["z"]
                        },
                        "orientation": {
                            "roll": self.state["orientation"]["roll"] + random.uniform(-0.01, 0.01),
                            "pitch": self.state["orientation"]["pitch"] + random.uniform(-0.01, 0.01),
                            "yaw": self.state["orientation"]["yaw"] + random.uniform(-0.01, 0.01)
                        },
                        "velocity": {
                            "x": self.state["velocity"]["x"] + random.uniform(-0.02, 0.02),
                            "y": self.state["velocity"]["y"] + random.uniform(-0.02, 0.02),
                            "z": self.state["velocity"]["z"]
                        },
                        "temperature": self.state["temperature"] + random.uniform(-0.1, 0.1),
                        "battery": self.state["battery"],
                        "status": self.state["status"]
                    }
                    
                    # Log telemetry
                    self.telemetry.log_telemetry(noisy_state)
                
                # Simulate sensor errors occasionally
                if random.random() < 0.01:  # ~1% chance
                    error_type = random.choice(["gps_dropout", "imu_error", "lidar_timeout"])
                    
                    # Log error
                    self.telemetry.log_error(
                        f"Sensor error: {error_type}",
                        error_type,
                        context={"position": self.state["position"]}
                    )
                    
                    # Add to state errors
                    self.state["errors"].append({
                        "type": error_type,
                        "timestamp": datetime.now().isoformat()
                    })
                    
                    # Clear old errors (keep last 5)
                    if len(self.state["errors"]) > 5:
                        self.state["errors"] = self.state["errors"][-5:]
                
                # Sleep for a short time
                time.sleep(0.1)
            
            except Exception as e:
                logger.error(f"Error in navigation worker: {e}")
                
                # Log error to telemetry
                self.telemetry.log_error(
                    f"Navigation error: {str(e)}",
                    "NavigationError",
                    stack_trace=traceback.format_exc(),
                    context={"state": self.state, "target": self.target}
                )
                
                time.sleep(1)
    
    def _sensor_worker(self):
        """Worker thread for sensor simulation"""
        while self.running:
            try:
                # Simulate sensor readings
                sensor_data = {
                    "infrared": [random.uniform(0.5, 4.0) for _ in range(5)],
                    "ultrasonic": random.uniform(0.5, 5.0),
                    "imu": {
                        "acceleration": {
                            "x": random.uniform(-0.1, 0.1),
                            "y": random.uniform(-0.1, 0.1),
                            "z": 9.8 + random.uniform(-0.1, 0.1)
                        },
                        "gyro": {
                            "x": random.uniform(-0.05, 0.05),
                            "y": random.uniform(-0.05, 0.05),
                            "z": random.uniform(-0.05, 0.05)
                        }
                    }
                }
                
                # Add obstacle detection occasionally
                if random.random() < 0.05:  # 5% chance
                    direction = random.randint(0, 4)  # Which IR sensor
                    distance = random.uniform(0.1, 0.3)  # Close distance
                    
                    sensor_data["infrared"][direction] = distance
                    
                    # Log obstacle event if we're moving
                    if abs(self.state["velocity"]["x"]) > 0.1 or abs(self.state["velocity"]["y"]) > 0.1:
                        self.telemetry.log_event(
                            "obstacle_detected",
                            {"direction": direction, "distance": distance, 
                             "position": self.state["position"]},
                            severity="WARN"
                        )
                
                # Update state with sensor readings
                self.state["sensors"] = sensor_data
                
                # Log detailed sensor data less frequently
                if random.random() < 0.1:  # ~10% chance
                    self.telemetry.log_telemetry({"sensors": sensor_data})
                
                # Sleep for a short time
                time.sleep(1)
            
            except Exception as e:
                logger.error(f"Error in sensor worker: {e}")
                time.sleep(5)
    
    def _battery_worker(self):
        """Worker thread for battery simulation"""
        # Battery parameters
        discharge_rate = 0.01  # % per second
        temperature_factor = 0.001  # Additional discharge per degree above 25°C
        
        while self.running:
            try:
                # Calculate discharge based on activity
                base_discharge = discharge_rate
                
                # Moving uses more battery
                velocity_magnitude = math.sqrt(
                    self.state["velocity"]["x"]**2 + 
                    self.state["velocity"]["y"]**2
                )
                movement_factor = velocity_magnitude * 0.02
                
                # Temperature affects battery
                temp_effect = max(0, (self.state["temperature"] - 25) * temperature_factor)
                
                # Calculate total discharge
                total_discharge = (base_discharge + movement_factor + temp_effect) * 1.0  # 1 second
                
                # Update battery level
                self.state["battery"] = max(0, self.state["battery"] - total_discharge)
                
                # Update temperature (correlates with activity)
                ambient_temp = 22.0
                activity_heat = movement_factor * 10
                cooling_factor = 0.1  # How fast temperature moves toward ambient
                
                self.state["temperature"] = self.state["temperature"] + (
                    (ambient_temp - self.state["temperature"]) * cooling_factor + 
                    activity_heat
                ) * 0.1
                
                # Log battery status periodically
                if random.random() < 0.05:  # ~5% chance
                    self.telemetry.log_metrics({
                        "battery_level": self.state["battery"],
                        "temperature": self.state["temperature"]
                    }, "power")
                
                # Generate low battery warnings
                if self.state["battery"] < 20 and random.random() < 0.2:
                    self.telemetry.log_event(
                        "low_battery",
                        {"level": self.state["battery"], 
                         "estimated_runtime": self.state["battery"] / discharge_rate},
                        severity="WARN"
                    )
                
                # Generate critical battery alert
                if self.state["battery"] < 10:
                    self.telemetry.log_event(
                        "critical_battery",
                        {"level": self.state["battery"], 
                         "estimated_runtime": self.state["battery"] / discharge_rate},
                        severity="ERROR"
                    )
                
                # Sleep for a short time
                time.sleep(1)
            
            except Exception as e:
                logger.error(f"Error in battery worker: {e}")
                time.sleep(5)


def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Example Robot with Telemetry')
    parser.add_argument('--robot-id', type=str, default="example-robot-001", help='Robot ID')
    parser.add_argument('--config', type=str, default="config.json", help='Path to config file')
    parser.add_argument('--patrol', action='store_true', help='Run a predefined patrol route')
    parser.add_argument('--battery-test', action='store_true', help='Run a battery drain test')
    args = parser.parse_args()
    
    # Create robot
    robot = ExampleRobot(args.robot_id, args.config)
    
    # Handle graceful shutdown
    def handle_shutdown(sig, frame):
        print("\nShutting down robot...")
        robot.stop()
        print("Shutdown complete.")
        exit(0)
    
    # Register signal handlers
    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)
    
    # Start robot
    robot.start()
    
    print(f"Robot {args.robot_id} started")
    print("Press Ctrl+C to stop")
    
    # Add demo tasks if requested
    if args.patrol:
        print("Starting patrol route...")
        
        # Define a simple patrol route
        patrol_points = [
            {"x": 10, "y": 10},
            {"x": 10, "y": -10},
            {"x": -10, "y": -10},
            {"x": -10, "y": 10}
        ]
        
        robot.add_task("patrol", {"points": patrol_points})
    
    elif args.battery_test:
        print("Starting battery drain test...")
        
        # Create a pattern that will drain the battery
        for i in range(10):
            x = 20 * math.cos(i * math.pi / 5)
            y = 20 * math.sin(i * math.pi / 5)
            robot.add_task("navigate", {"x": x, "y": y})
    
    else:
        # Add some demo tasks
        robot.add_task("navigate", {"x": 5, "y": 5})
        robot.add_task("navigate", {"x": -5, "y": 5})
        robot.add_task("navigate", {"x": -5, "y": -5})
        robot.add_task("navigate", {"x": 5, "y": -5})
        robot.add_task("navigate", {"x": 0, "y": 0})
    
    # Keep main thread running
    try:
        while True:
            # Display occasional status updates
            if robot.state["status"] == "navigating":
                pos = robot.state["position"]
                target = robot.target
                if target:
                    distance = math.sqrt(
                        (pos["x"] - target["x"])**2 + 
                        (pos["y"] - target["y"])**2
                    )
                    print(f"\rPosition: ({pos['x']:.2f}, {pos['y']:.2f}) - "
                          f"Target: ({target['x']:.2f}, {target['y']:.2f}) - "
                          f"Distance: {distance:.2f} - "
                          f"Battery: {robot.state['battery']:.1f}%", end="")
            
            time.sleep(0.5)
    
    except KeyboardInterrupt:
        pass
    
    # Stop robot on exit
    robot.stop()


if __name__ == "__main__":
    import traceback
    
    try:
        main()
    except Exception as e:
        logger.critical(f"Unhandled exception: {e}")
        traceback.print_exc()
        sys.exit(1)