import sqlite3
import logging
from pathlib import Path
from typing import Optional, List, Dict, Any
import pandas as pd

class DatabaseManager:
    """
    Database connection and utility manager for NFL fantasy data
    """
    
    def __init__(self, db_path: str = None):
        """Initialize database connection
        
        Args:
            db_path: Path to SQLite database file. If None, uses default path
        """
        if db_path is None:
            project_root = Path(__file__).parent.parent.parent
            data_dir = project_root / "data"
            data_dir.mkdir(exist_ok=True)
            db_path = data_dir / "nfl_fantasy.db"
        
        self.db_path = str(db_path)
        self.connection = None
        self.setup_logging()
    
    def setup_logging(self):
        """Setup logging for database operations"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
    
    def connect(self) -> sqlite3.Connection:
        """Establish database connection"""
        try:
            self.connection = sqlite3.connect(self.db_path)
            self.connection.row_factory = sqlite3.Row  # Enable column access by name
            self.logger.info(f"Connected to database: {self.db_path}")
            return self.connection
        except Exception as e:
            self.logger.error(f"Failed to connect to database: {e}")
            raise
    
    def disconnect(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            self.connection = None
            self.logger.info("Database connection closed")
    
    def execute_schema(self, schema_file_path: str = None):
        """Execute database schema from SQL file
        
        Args:
            schema_file_path: Path to schema SQL file
        """
        if schema_file_path is None:
            schema_file_path = Path(__file__).parent.parent.parent / "database_schema.sql"
        
        try:
            with open(schema_file_path, 'r') as file:
                schema_sql = file.read()
            
            if not self.connection:
                self.connect()
            
            # Execute schema in chunks (split by semicolon)
            statements = [stmt.strip() for stmt in schema_sql.split(';') if stmt.strip()]
            
            for statement in statements:
                self.connection.execute(statement)
            
            self.connection.commit()
            self.logger.info("Database schema executed successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to execute schema: {e}")
            if self.connection:
                self.connection.rollback()
            raise
    
    def insert_data(self, table_name: str, data: Dict[str, Any]) -> int:
        """Insert single record into table
        
        Args:
            table_name: Name of the table
            data: Dictionary of column_name: value
            
        Returns:
            ID of inserted record
        """
        try:
            columns = ', '.join(data.keys())
            placeholders = ', '.join(['?' for _ in data])
            values = tuple(data.values())
            
            sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
            
            if not self.connection:
                self.connect()
            
            cursor = self.connection.execute(sql, values)
            self.connection.commit()
            
            self.logger.debug(f"Inserted record into {table_name} with ID {cursor.lastrowid}")
            return cursor.lastrowid
            
        except Exception as e:
            self.logger.error(f"Failed to insert into {table_name}: {e}")
            if self.connection:
                self.connection.rollback()
            raise
    
    def insert_bulk_data(self, table_name: str, data: List[Dict[str, Any]]) -> int:
        """Insert multiple records into table
        
        Args:
            table_name: Name of the table
            data: List of dictionaries with column_name: value
            
        Returns:
            Number of records inserted
        """
        if not data:
            return 0
        
        try:
            # Use first record to determine columns
            columns = ', '.join(data[0].keys())
            placeholders = ', '.join(['?' for _ in data[0]])
            
            sql = f"INSERT OR IGNORE INTO {table_name} ({columns}) VALUES ({placeholders})"
            
            if not self.connection:
                self.connect()
            
            # Convert dictionaries to tuples maintaining column order
            values_list = [tuple(record.values()) for record in data]
            
            cursor = self.connection.executemany(sql, values_list)
            self.connection.commit()
            
            rows_inserted = cursor.rowcount
            self.logger.info(f"Inserted {rows_inserted} records into {table_name}")
            return rows_inserted
            
        except Exception as e:
            self.logger.error(f"Failed to bulk insert into {table_name}: {e}")
            if self.connection:
                self.connection.rollback()
            raise
    
    def query(self, sql: str, params: tuple = None) -> List[sqlite3.Row]:
        """Execute SELECT query and return results
        
        Args:
            sql: SQL query string
            params: Query parameters
            
        Returns:
            List of Row objects
        """
        try:
            if not self.connection:
                self.connect()
            
            if params:
                cursor = self.connection.execute(sql, params)
            else:
                cursor = self.connection.execute(sql)
            
            results = cursor.fetchall()
            self.logger.debug(f"Query returned {len(results)} rows")
            return results
            
        except Exception as e:
            self.logger.error(f"Query failed: {e}")
            raise
    
    def query_to_dataframe(self, sql: str, params: tuple = None) -> pd.DataFrame:
        """Execute query and return results as pandas DataFrame
        
        Args:
            sql: SQL query string
            params: Query parameters
            
        Returns:
            pandas DataFrame
        """
        try:
            if not self.connection:
                self.connect()
            
            df = pd.read_sql_query(sql, self.connection, params=params)
            self.logger.debug(f"Query returned DataFrame with shape {df.shape}")
            return df
            
        except Exception as e:
            self.logger.error(f"Query to DataFrame failed: {e}")
            raise
    
    def table_exists(self, table_name: str) -> bool:
        """Check if table exists in database
        
        Args:
            table_name: Name of table to check
            
        Returns:
            True if table exists, False otherwise
        """
        try:
            sql = "SELECT name FROM sqlite_master WHERE type='table' AND name=?"
            result = self.query(sql, (table_name,))
            return len(result) > 0
        except Exception:
            return False
    
    def get_table_count(self, table_name: str) -> int:
        """Get row count for a table
        
        Args:
            table_name: Name of table
            
        Returns:
            Number of rows in table
        """
        try:
            sql = f"SELECT COUNT(*) as count FROM {table_name}"
            result = self.query(sql)
            return result[0]['count'] if result else 0
        except Exception as e:
            self.logger.error(f"Failed to get count for {table_name}: {e}")
            return 0
    
    def upsert_data(self, table_name: str, data: Dict[str, Any], 
                   conflict_columns: List[str]) -> int:
        """Insert or update record (upsert)
        
        Args:
            table_name: Name of the table
            data: Dictionary of column_name: value
            conflict_columns: List of columns that define uniqueness
            
        Returns:
            ID of inserted/updated record
        """
        try:
            columns = ', '.join(data.keys())
            placeholders = ', '.join(['?' for _ in data])
            values = tuple(data.values())
            
            # Build conflict resolution
            conflict_cols = ', '.join(conflict_columns)
            update_set = ', '.join([f"{col} = excluded.{col}" 
                                  for col in data.keys() 
                                  if col not in conflict_columns])
            
            sql = f"""
                INSERT INTO {table_name} ({columns}) VALUES ({placeholders})
                ON CONFLICT({conflict_cols}) DO UPDATE SET {update_set}
            """
            
            if not self.connection:
                self.connect()
            
            cursor = self.connection.execute(sql, values)
            self.connection.commit()
            
            return cursor.lastrowid
            
        except Exception as e:
            self.logger.error(f"Failed to upsert into {table_name}: {e}")
            if self.connection:
                self.connection.rollback()
            raise
    
    def __enter__(self):
        """Context manager entry"""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.disconnect()


# Utility functions
def get_database() -> DatabaseManager:
    """Get default database instance"""
    return DatabaseManager()


def initialize_database(schema_file: str = None) -> DatabaseManager:
    """Initialize database with schema
    
    Args:
        schema_file: Path to schema file
        
    Returns:
        Initialized DatabaseManager instance
    """
    db = DatabaseManager()
    db.connect()
    db.execute_schema(schema_file)
    return db