"""
Local Buffer for Robot Telemetry

This module provides a persistent local storage buffer using SQLite.
It ensures data is not lost during connectivity issues.
"""

import json
import os
import sqlite3
import logging
import zlib
import time
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Tuple, Union

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("telemetry-local-buffer")

class LocalBuffer:
    """SQLite-based local buffer for telemetry data"""
    
    def __init__(self, 
                robot_id: str, 
                db_path: Optional[str] = None,
                compression_enabled: bool = True,
                max_db_size_mb: int = 1000,
                retention_days: int = 30):
        """
        Initialize the local buffer
        
        Args:
            robot_id: Unique identifier for the robot
            db_path: Path to the SQLite database file
            compression_enabled: Whether to compress data before storing
            max_db_size_mb: Maximum size of the database in MB
            retention_days: How many days to retain data
        """
        self.robot_id = robot_id
        self.db_path = db_path or f"telemetry_{robot_id}.db"
        self.compression_enabled = compression_enabled
        self.max_db_size_mb = max_db_size_mb
        self.retention_days = retention_days
        
        # Ensure the directory exists
        os.makedirs(os.path.dirname(os.path.abspath(self.db_path)), exist_ok=True)
        
        # Initialize the database
        self._init_db()
        
        logger.info(f"Local buffer initialized for robot {robot_id} at {self.db_path}")
    
    def _init_db(self):
        """Initialize the SQLite database"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Create telemetry buffer table
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS telemetry_buffer (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                data BLOB NOT NULL,
                compressed INTEGER DEFAULT 0,
                sent INTEGER DEFAULT 0,
                retries INTEGER DEFAULT 0,
                last_attempt TEXT,
                data_type TEXT,
                priority INTEGER DEFAULT 1
            )
            ''')
            
            # Create indices for efficient querying
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_sent ON telemetry_buffer(sent)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_timestamp ON telemetry_buffer(timestamp)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_data_type ON telemetry_buffer(data_type)')
            
            # Create statistics table
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS buffer_stats (
                key TEXT PRIMARY KEY,
                value TEXT
            )
            ''')
            
            conn.commit()
            conn.close()
        except sqlite3.Error as e:
            logger.error(f"Database initialization error: {e}")
            raise
    
    def _compress_data(self, data_json: str) -> Tuple[Union[str, bytes], bool]:
        """
        Compress JSON data if compression is enabled
        
        Args:
            data_json: JSON string to compress
            
        Returns:
            Tuple of (compressed_data, is_compressed)
        """
        if not self.compression_enabled:
            return data_json, False
        
        try:
            compressed = zlib.compress(data_json.encode('utf-8'))
            return compressed, True
        except Exception as e:
            logger.warning(f"Data compression failed: {e}")
            return data_json, False
    
    def _decompress_data(self, data: Union[str, bytes], compressed: bool) -> str:
        """
        Decompress data if it was compressed
        
        Args:
            data: Data to decompress
            compressed: Whether the data is compressed
            
        Returns:
            Decompressed data as a string
        """
        if not compressed:
            return data if isinstance(data, str) else data.decode('utf-8')
        
        try:
            if isinstance(data, str):
                data = data.encode('utf-8')
            return zlib.decompress(data).decode('utf-8')
        except Exception as e:
            logger.error(f"Data decompression failed: {e}")
            # Return something rather than failing completely
            return str(data)
    
    def store(self, 
             data: Dict[str, Any], 
             priority: int = 1) -> bool:
        """
        Store telemetry data in the local buffer
        
        Args:
            data: The telemetry data to store
            priority: Priority level (higher = more important)
            
        Returns:
            Whether the operation was successful
        """
        try:
            # Convert to JSON
            data_json = json.dumps(data)
            
            # Get timestamp
            timestamp = data.get('timestamp', datetime.now().isoformat())
            
            # Get data type
            data_type = data.get('data_type', 'telemetry')
            
            # Compress if enabled
            compressed_data, is_compressed = self._compress_data(data_json)
            
            # Store in database
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute(
                '''
                INSERT INTO telemetry_buffer 
                (timestamp, data, compressed, data_type, priority) 
                VALUES (?, ?, ?, ?, ?)
                ''',
                (timestamp, sqlite3.Binary(compressed_data) if is_compressed else compressed_data, 
                 int(is_compressed), data_type, priority)
            )
            
            # Update statistics
            self._update_stat(cursor, 'total_entries', lambda x: int(x) + 1 if x else 1)
            self._update_stat(cursor, 'last_entry_time', lambda x: datetime.now().isoformat())
            
            conn.commit()
            conn.close()
            
            # Check if database needs maintenance
            self._check_maintenance()
            
            return True
        except Exception as e:
            logger.error(f"Error storing data: {e}")
            return False
    
    def _update_stat(self, cursor, key, value_func):
        """Update a statistic in the database"""
        cursor.execute('SELECT value FROM buffer_stats WHERE key = ?', (key,))
        row = cursor.fetchone()
        
        if row:
            current_value = row[0]
            new_value = value_func(current_value)
        else:
            new_value = value_func(None)
        
        cursor.execute(
            'INSERT OR REPLACE INTO buffer_stats (key, value) VALUES (?, ?)',
            (key, str(new_value))
        )
    
    def get_unsent(self, 
                  limit: int = 100, 
                  data_type: Optional[str] = None,
                  min_priority: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Get unsent data from the buffer
        
        Args:
            limit: Maximum number of entries to retrieve
            data_type: Optional filter by data type
            min_priority: Optional minimum priority
            
        Returns:
            List of unsent data entries
        """
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            # Build query
            query = 'SELECT * FROM telemetry_buffer WHERE sent = 0'
            params = []
            
            if data_type:
                query += ' AND data_type = ?'
                params.append(data_type)
            
            if min_priority is not None:
                query += ' AND priority >= ?'
                params.append(min_priority)
            
            # Order by priority (higher first), then timestamp
            query += ' ORDER BY priority DESC, timestamp ASC LIMIT ?'
            params.append(limit)
            
            cursor.execute(query, params)
            rows = cursor.fetchall()
            
            # Process results
            result = []
            for row in rows:
                # Decompress if needed
                data_str = self._decompress_data(row['data'], bool(row['compressed']))
                
                try:
                    # Parse JSON
                    data_dict = json.loads(data_str)
                    
                    # Add buffer metadata
                    result.append({
                        'id': row['id'],
                        'timestamp': row['timestamp'],
                        'data': data_dict,
                        'data_type': row['data_type'],
                        'priority': row['priority'],
                        'retries': row['retries']
                    })
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to parse JSON for entry {row['id']}: {e}")
            
            conn.close()
            return result
        except sqlite3.Error as e:
            logger.error(f"Error retrieving unsent data: {e}")
            return []
    
    def mark_as_sent(self, entry_ids: List[int]) -> bool:
        """
        Mark entries as successfully sent
        
        Args:
            entry_ids: List of entry IDs to mark
            
        Returns:
            Whether the operation was successful
        """
        if not entry_ids:
            return True
        
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Create placeholders for the query
            placeholders = ','.join('?' for _ in entry_ids)
            
            # Update the entries
            cursor.execute(
                f'UPDATE telemetry_buffer SET sent = 1 WHERE id IN ({placeholders})',
                entry_ids
            )
            
            # Update statistics
            '''
            cursor.rowcount is a property of a database cursor object that returns the 
            number of rows that were affected by the last SQL statement that was 
            executed using that cursor.
            '''
            sent_count = cursor.rowcount
            self._update_stat(cursor, 'sent_entries', 
                             lambda x: int(x) + sent_count if x else sent_count)
            self._update_stat(cursor, 'last_sent_time', 
                             lambda x: datetime.now().isoformat())
            
            conn.commit()
            conn.close()
            
            logger.info(f"Marked {sent_count} entries as sent")
            return True
        except sqlite3.Error as e:
            logger.error(f"Error marking entries as sent: {e}")
            return False
    
    '''
    Do we need this locally?
    '''
    def update_retry(self, entry_ids: List[int]) -> bool:
        """
        Update retry counter for failed entries
        
        Args:
            entry_ids: List of entry IDs to update
            
        Returns:
            Whether the operation was successful
        """
        if not entry_ids:
            return True
        
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Create placeholders for the query
            placeholders = ','.join('?' for _ in entry_ids)
            
            # Update the entries
            now = datetime.now().isoformat()
            cursor.execute(
                f'''
                UPDATE telemetry_buffer 
                SET retries = retries + 1, last_attempt = ? 
                WHERE id IN ({placeholders})
                ''',
                [now] + entry_ids
            )
            
            # Update statistics
            retry_count = cursor.rowcount
            self._update_stat(cursor, 'retry_count', 
                             lambda x: int(x) + retry_count if x else retry_count)
            
            conn.commit()
            conn.close()
            
            logger.info(f"Updated retry count for {retry_count} entries")
            return True
        except sqlite3.Error as e:
            logger.error(f"Error updating retry count: {e}")
            return False
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get buffer statistics
        
        Returns:
            Dictionary of statistics
        """
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Get all stats
            cursor.execute('SELECT key, value FROM buffer_stats')
            stats = {row[0]: row[1] for row in cursor.fetchall()}
            
            # Get count of total entries
            cursor.execute('SELECT COUNT(*) FROM telemetry_buffer')
            stats['total_entries'] = cursor.fetchone()[0]
            
            # Get count of unsent entries
            cursor.execute('SELECT COUNT(*) FROM telemetry_buffer WHERE sent = 0')
            stats['unsent_entries'] = cursor.fetchone()[0]
            
            # Get database size
            db_size = os.path.getsize(self.db_path) if os.path.exists(self.db_path) else 0
            stats['db_size_bytes'] = db_size
            stats['db_size_mb'] = round(db_size / (1024 * 1024), 2)
            
            # Get counts by data type
            cursor.execute(
                '''
                SELECT data_type, COUNT(*) 
                FROM telemetry_buffer 
                GROUP BY data_type
                '''
            )
            stats['entries_by_type'] = {row[0]: row[1] for row in cursor.fetchall()}
            
            conn.close()
            return stats
        except (sqlite3.Error, OSError) as e:
            logger.error(f"Error getting stats: {e}")
            return {}
    
    def clean_old_data(self) -> int:
        """
        Remove old data that has been successfully sent
        
        Returns:
            Number of entries deleted
        """
        if self.retention_days <= 0:
            return 0  # No cleanup if retention is disabled
        
        try:
            # Calculate cutoff date
            cutoff = (datetime.now() - timedelta(days=self.retention_days)).isoformat()
            
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Delete old sent entries
            cursor.execute(
                'DELETE FROM telemetry_buffer WHERE timestamp < ? AND sent = 1',
                (cutoff,)
            )
            
            deleted_count = cursor.rowcount
            
            # Update stats
            if deleted_count > 0:
                self._update_stat(cursor, 'last_cleanup_time', 
                                 lambda x: datetime.now().isoformat())
                self._update_stat(cursor, 'last_cleanup_count', 
                                 lambda x: str(deleted_count))
            
            conn.commit()
            conn.close()
            
            if deleted_count > 0:
                logger.info(f"Cleaned up {deleted_count} old entries")
            
            return deleted_count
        except sqlite3.Error as e:
            logger.error(f"Error cleaning old data: {e}")
            return 0
    
    def _check_maintenance(self):
        """Check if database maintenance is needed and perform if necessary"""
        try:
            # Check database size
            if os.path.exists(self.db_path):
                db_size_mb = os.path.getsize(self.db_path) / (1024 * 1024)
                
                # If database is getting large, run maintenance
                if db_size_mb > self.max_db_size_mb * 0.8:
                    logger.warning(f"Database size ({db_size_mb:.2f} MB) approaching limit, running maintenance")
                    
                    # Clean old data first
                    self.clean_old_data()
                    
                    # If still too large, force cleanup of older sent entries
                    if os.path.getsize(self.db_path) / (1024 * 1024) > self.max_db_size_mb * 0.8:
                        self._force_cleanup()
        except Exception as e:
            logger.error(f"Error during maintenance check: {e}")
    
    def _force_cleanup(self):
        """Force cleanup of older sent entries when database is too large"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # First, get count of sent entries
            cursor.execute('SELECT COUNT(*) FROM telemetry_buffer WHERE sent = 1')
            sent_count = cursor.fetchone()[0]
            
            if sent_count > 100:  # Only proceed if we have a meaningful number of sent entries
                # Delete the oldest 25% of sent entries
                to_delete = max(int(sent_count * 0.25), 100)  # At least 100
                
                # Find the cutoff timestamp
                cursor.execute(
                    '''
                    SELECT timestamp FROM telemetry_buffer
                    WHERE sent = 1
                    ORDER BY timestamp ASC
                    LIMIT 1 OFFSET ?
                    ''',
                    (to_delete - 1,)
                )
                
                result = cursor.fetchone()
                if result:
                    cutoff_timestamp = result[0]
                    
                    # Delete entries older than the cutoff
                    cursor.execute(
                        'DELETE FROM telemetry_buffer WHERE timestamp <= ? AND sent = 1',
                        (cutoff_timestamp,)
                    )
                    
                    deleted_count = cursor.rowcount
                    logger.warning(f"Force cleaned {deleted_count} sent entries to reduce database size")
            
            conn.commit()
            
            # Optimize database
            if sent_count > 1000:  # Only vacuum for larger databases
                logger.info("Running VACUUM to reclaim space")
                cursor.execute('VACUUM')
            
            conn.close()
        except sqlite3.Error as e:
            logger.error(f"Error during forced cleanup: {e}")
    
    def optimize(self):
        """Optimize the database"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Update statistics first
            cursor.execute('ANALYZE')
            
            # Then vacuum to reclaim space
            cursor.execute('VACUUM')
            
            conn.close()
            
            logger.info("Database optimized")
            return True
        except sqlite3.Error as e:
            logger.error(f"Error optimizing database: {e}")
            return False


# Example usage
if __name__ == "__main__":
    # Create buffer
    buffer = LocalBuffer("robot-001", db_path="data/telemetry_robot001.db")
    
    # Store some test data
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
        
        buffer.store(test_data)
        time.sleep(0.1)  # Small delay for different timestamps
    
    # Store a high priority item
    high_priority_data = {
        "robot_id": "robot-001",
        "timestamp": datetime.now().isoformat(),
        "data_type": "alert",
        "data": {
            "alert_type": "low_battery",
            "battery": 15,
            "message": "Battery level critical"
        }
    }
    buffer.store(high_priority_data, priority=10)
    
    # Get unsent data
    unsent = buffer.get_unsent(limit=10)
    print(f"Found {len(unsent)} unsent entries")
    
    for entry in unsent:
        print(f"Entry {entry['id']}: {entry['data_type']} priority={entry['priority']}")
        
        # Just to demo, mark some as sent
        if entry['id'] % 2 == 0:
            buffer.mark_as_sent([entry['id']])
        else:
            buffer.update_retry([entry['id']])
    
    # Show stats
    stats = buffer.get_stats()
    print("\nBuffer Statistics:")
    for key, value in stats.items():
        print(f"  {key}: {value}")
    
    # Clean up
    deleted = buffer.clean_old_data()
    print(f"\nCleaned up {deleted} old entries")
    
    # Optimize
    buffer.optimize()