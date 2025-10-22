import os
import logging
from typing import Optional, Dict, List, Tuple, Any

import yaml
import snowflake.connector
from snowflake.connector.errors import ProgrammingError, OperationalError

logger = logging.getLogger(__name__)

TIMESTAMP_TYPES = {'TIMESTAMP_TZ', 'TIMESTAMP_LTZ'}
BASE_TABLE_TYPE = 'BASE TABLE'
VIEW_TYPE = 'VIEW'


class SnowflakeConnectionError(Exception):
    """Raised when Snowflake connection fails."""
    pass


class SnowflakeQueryError(Exception):
    """Raised when Snowflake query execution fails."""
    pass


class SnowflakeClient:
    """
    Manages Snowflake database connections and operations.
    
    Provides functionality to:
    - Connect to Snowflake using connection profiles
    - Query table metadata from YAML configurations
    - Generate and execute COPY queries for data export
    - Handle database context switching
    """

    def __init__(self, connection_name: str = "default"):
        """
        Initialize the Snowflake client.

        Args:
            connection_name: Name of the connection profile in connections.toml
        """
        self.connection_name = connection_name
        self.conn: Optional[snowflake.connector.connection.SnowflakeConnection] = None
        self.cursor: Optional[snowflake.connector.cursor.SnowflakeCursor] = None
        self.current_db: Optional[str] = None

    def __enter__(self):
        """Connect to Snowflake when entering context manager."""
        try:
            logger.info(f"Connecting to Snowflake using connection: '{self.connection_name}'...")
            self.conn = snowflake.connector.connect(connection_name=self.connection_name)
            self.cursor = self.conn.cursor()
            logger.info("Connected successfully.")
            return self
        except OperationalError as e:
            logger.error(f"Snowflake connection failed: {e}")
            raise SnowflakeConnectionError(f"Failed to connect using '{self.connection_name}': {e}")
        except Exception as e:
            logger.error(f"Unexpected error during connection: {e}")
            raise SnowflakeConnectionError(f"Unexpected connection error: {e}")

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Close Snowflake connection when exiting context manager."""
        if self.conn:
            try:
                self.conn.close()
                logger.info("Snowflake connection closed.")
            except Exception as e:
                logger.warning(f"Error closing connection: {e}")
        return False

    def _ensure_connection(self) -> None:
        """Ensure we have an active connection."""
        if not self.conn:
            raise SnowflakeConnectionError("No active Snowflake connection")

    def _switch_database_if_needed(self, database: str) -> None:
        """Switch to database if different from current."""
        if database != self.current_db:
            logger.debug(f"Switching database to: {database}")
            try:
                self.cursor.execute(f"USE DATABASE {database}")
                self.current_db = database
            except Exception as e:
                logger.error(f"Failed to switch to database {database}: {e}")
                raise SnowflakeQueryError(f"Database switch failed: {e}")

    def execute_query(self, query: str) -> List[Dict]:
        """
        Execute a SQL query and return results as list of dictionaries.

        Args:
            query: SQL query to execute

        Returns:
            List of dictionaries containing query results

        Raises:
            SnowflakeQueryError: If query execution fails
        """
        self._ensure_connection()
        
        try:
            self.cursor.execute(query)
            logger.debug(f"Query executed successfully: {query[:100]}...")
            return self.cursor.fetch_pandas_all().to_dict('records')
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            raise SnowflakeQueryError(f"Query failed: {e}")

    def _load_yaml_config(self, file_path: str) -> List[Dict[str, Any]]:
        """Load and validate YAML configuration file."""
        try:
            with open(file_path, 'r') as file:
                data = yaml.safe_load(file)
        except FileNotFoundError:
            raise FileNotFoundError(f"Configuration file '{file_path}' not found")
        except yaml.YAMLError as e:
            raise ValueError(f"Invalid YAML in '{file_path}': {e}")

        if not isinstance(data, list):
            raise ValueError("YAML file must contain a list of table configurations")

        return data

    def _build_table_query(self, config: Dict[str, Any]) -> str:
        """Build SQL query to get table metadata based on configuration."""
        database = config['database']
        schema = config.get('schema')
        table = config.get('table')
        exclude_schema_like = config.get('exclude_schema_like', [])
        exclude_table_like = config.get('exclude_table_like', [])
        with_views = config.get('with_views', False)

        base_query = f"""
            SELECT 
                c.table_catalog AS database_name,
                c.table_schema AS schema_name,
                c.table_name,
                c.column_name,
                c.data_type,
                t.table_type AS table_type
            FROM 
                {database}.INFORMATION_SCHEMA.COLUMNS c
            JOIN
                {database}.INFORMATION_SCHEMA.TABLES t
                ON c.table_name = t.table_name
                AND c.table_schema = t.table_schema
        """

        where_clauses = []

        if not with_views:
            where_clauses.append(f"t.table_type = '{BASE_TABLE_TYPE}'")

        if schema:
            where_clauses.append(f"c.table_schema = '{schema.upper()}'")
        if table:
            where_clauses.append(f"c.table_name = '{table.upper()}'")

        for pattern in exclude_schema_like:
            if pattern:
                where_clauses.append(f"c.table_schema NOT LIKE '{pattern.upper()}'")
        
        for pattern in exclude_table_like:
            if pattern:
                where_clauses.append(f"c.table_name NOT LIKE '{pattern.upper()}'")

        full_query = base_query
        if where_clauses:
            full_query += " WHERE " + " AND ".join(where_clauses)

        full_query += " ORDER BY c.table_schema, c.table_name, c.ordinal_position"
        return full_query

    def _validate_config_item(self, item: Dict[str, Any]) -> bool:
        """Validate a single configuration item."""
        if not isinstance(item, dict):
            logger.error("Each item in the configuration list should be an object")
            return False

        database = item.get('database')
        schema = item.get('schema')
        table = item.get('table')

        if not database:
            logger.warning(f"Skipping entry: 'database' is mandatory. Item: {item}")
            return False

        if table and not schema:
            logger.warning(f"Skipping entry: 'schema' is mandatory if 'table' is provided. Item: {item}")
            return False

        return True

    def _process_query_results(self, results: List[Dict]) -> Dict[Tuple[str, str, str], Dict[str, Any]]:
        """Process query results into organized table structure."""
        tables_with_columns = {}

        for row in results:
            db_name = row['DATABASE_NAME']
            schema_name = row['SCHEMA_NAME']
            table_name = row['TABLE_NAME']
            column_name = row['COLUMN_NAME']
            data_type = row['DATA_TYPE']
            table_type = row['TABLE_TYPE']

            table_key = (db_name, schema_name, table_name)

            if table_key not in tables_with_columns:
                tables_with_columns[table_key] = {}
                tables_with_columns[table_key]['database'] = db_name
                tables_with_columns[table_key]['schema'] = schema_name
                tables_with_columns[table_key]['table'] = table_name
                tables_with_columns[table_key]['table_type'] = table_type

                tables_with_columns[table_key]['columns'] = []

            column_info = {
                'column_name': column_name,
                'data_type': data_type
            }
            tables_with_columns[table_key]['columns'].append(column_info)

        return tables_with_columns

    def list_tables_from_yaml(self, file_path: str) -> List[Dict[str, Any]]:
        """
        Parse YAML configuration and query Snowflake for table metadata.

        Args:
            file_path: Path to the YAML configuration file

        Returns:
            List of dictionaries, each representing a table with its columns

        Raises:
            FileNotFoundError: If YAML file is not found
            ValueError: If YAML file is invalid
            SnowflakeQueryError: If database queries fail
        """
        self._ensure_connection()
        
        config_items = self._load_yaml_config(file_path)
        
        all_tables = {}
        
        logger.info("Processing table configuration...")
        for item in config_items:
            if not self._validate_config_item(item):
                continue

            try:
                query = self._build_table_query(item)
                logger.info(f"Querying tables for database: {item['database']}")
                
                query_results = self.execute_query(query)
                
                table_data = self._process_query_results(query_results)
                
                all_tables.update(table_data)
                
            except (ProgrammingError, SnowflakeQueryError) as e:
                logger.error(f"Database error for item {item}: {e}")
                continue
            except Exception as e:
                logger.error(f"Unexpected error processing item {item}: {e}")
                continue

        return list(all_tables.values())

    def _normalize_column_name_for_bigquery(self, column_name: str) -> str:
        """
        Normalize column name using BigQuery V2 character mapping rules.
        This matches the behavior of column_name_character_map="V2" in BigQuery load jobs.
        
        Args:
            column_name: Original column name (with or without quotes)
            
        Returns:
            Normalized column name that follows BigQuery naming conventions
        """
        if not column_name:
            return '_'
            
        name = column_name.strip()
        if name.startswith('"') and name.endswith('"'):
            name = name[1:-1]
        name = name.strip()
        
        name = name.replace('""', '"')
        
        normalized = ''
        for char in name:
            if char.isalnum() or char == '_':
                normalized += char
            else:
                normalized += '_'
        
        if normalized and normalized[0].isdigit():
            normalized = '_' + normalized
            
        if not normalized or not (normalized[0].isalpha() or normalized[0] == '_'):
            normalized = '_' + normalized if normalized else '_'
            
        while '__' in normalized:
            normalized = normalized.replace('__', '_')
        normalized = normalized.rstrip('_')
        
        if not normalized or normalized == '_':
            normalized = '_'
            
        if len(normalized) > 300:
            normalized = normalized[:300]
            
        reserved_prefixes = ['_TABLE_', '_FILE_', '_PARTITION_']
        for prefix in reserved_prefixes:
            if normalized.upper().startswith(prefix):
                normalized = normalized + '_'
                break

        normalized = normalized.lower()

        logger.debug(f"Normalized column name: '{column_name}' -> '{normalized}'")
        return normalized

    def _build_column_expression(self, column: Dict[str, str], cast_timestamp_to_string: bool = True) -> str:
        """Build a column expression for SELECT query."""
        col_name = column["column_name"]
        data_type = column["data_type"]
        
        quoted_col = f'"{col_name.replace('"', '""')}"'
        
        normalized_alias = self._normalize_column_name_for_bigquery(quoted_col)
        quoted_alias = f'"{normalized_alias}"'
        
        if cast_timestamp_to_string and data_type in TIMESTAMP_TYPES:
            return f'{quoted_col}::TIMESTAMP_NTZ AS {quoted_alias}'
        else:
            return f'{quoted_col} AS {quoted_alias}'

    def _build_gcs_path(self, database: str, schema: str, table: str) -> str:
        """Build GCS path for table data."""
        return f"{database.lower()}/{schema.lower()}/{table.lower()}/"

    def generate_copy_query(
        self, 
        external_stage: str, 
        db: str, 
        schema: str, 
        table: str,
        columns: List[Dict[str, str]], 
        sample: bool = False, 
        cast_timestamp_to_string: bool = True
    ) -> str:
        """
        Generate a Snowflake COPY INTO query to unload table data to GCS.

        Args:
            external_stage: Name of the external stage
            db: Database name
            schema: Schema name  
            table: Table name
            columns: List of column info dicts with 'column_name' and 'data_type'
            sample: If True, limit rows to SAMPLE_LIMIT environment variable
            cast_timestamp_to_string: If True, cast timestamp columns to strings

        Returns:
            Formatted COPY INTO query string
        """
        sample_limit = int(os.getenv('SAMPLE_LIMIT', 100))
        fully_qualified_table = f"{db}.{schema}.{table}"

        column_expressions = [
            self._build_column_expression(col, cast_timestamp_to_string) 
            for col in columns
        ]
        
        gcs_path = self._build_gcs_path(db, schema, table)

        formatted_select = f"""    SELECT
        {(',\n        '.join(column_expressions))}
    FROM {fully_qualified_table}"""
        
        if sample:
            formatted_select += f"\n    LIMIT {sample_limit}"

        query = f"""COPY INTO @{external_stage}/{gcs_path}
FROM (
{formatted_select}
)
FILE_FORMAT = (TYPE = PARQUET, SNAPPY_COMPRESSION = TRUE)
OVERWRITE = TRUE
HEADER = TRUE;"""

        return query

    def _build_cleaning_query(self, external_stage: str, db: str, schema: str, table: str) -> str:
        """Build REMOVE query to clean existing data from GCS."""
        gcs_path = self._build_gcs_path(db, schema, table)
        return f"REMOVE @{external_stage}/{gcs_path}"

    def generate_copy_queries(
        self, 
        table_list: List[Dict[str, Any]], 
        external_stage: str, 
        sample: bool = False,
        cast_timestamp_to_string: bool = True
    ) -> List[Dict[str, Any]]:
        """
        Generate COPY queries for a list of tables.

        Args:
            table_list: List of table dictionaries
            external_stage: Name of the external stage
            sample: Whether to sample data (limit rows)
            cast_timestamp_to_string: Whether to cast timestamps to strings

        Returns:
            Updated table list with 'copy_query' added to each table
        """
        for table_info in table_list:
            try:
                cleaning_query = self._build_cleaning_query(external_stage,
                                                           table_info['database'],
                                                           table_info['schema'],
                                                           table_info['table'])

                copy_query = self.generate_copy_query(
                    external_stage=external_stage,
                    db=table_info['database'],
                    schema=table_info['schema'],
                    table=table_info['table'],
                    columns=table_info['columns'],
                    sample=sample,
                    cast_timestamp_to_string=cast_timestamp_to_string
                )

                ordered_table_info = {
                    'database': table_info['database'],
                    'schema': table_info['schema'],
                    'table': table_info['table'],
                    'table_type': table_info.get('table_type'),
                    'columns': table_info['columns'],
                    'cleaning_query': cleaning_query,
                    'copy_query': copy_query
                }
                
                table_info.clear()
                table_info.update(ordered_table_info)
                
            except Exception as e:
                logger.error(f"Failed to generate copy query for {table_info.get('database', '')}.{table_info.get('schema', '')}.{table_info.get('table', '')}: {e}")
                cleaning_query = self._build_cleaning_query(external_stage, 
                                                           table_info.get('database', ''), 
                                                           table_info.get('schema', ''), 
                                                           table_info.get('table', ''))
                ordered_table_info = {
                    'database': table_info.get('database'),
                    'schema': table_info.get('schema'),
                    'table': table_info.get('table'),
                    'table_type': table_info.get('table_type'),
                    'columns': table_info.get('columns', []),
                    'cleaning_query': cleaning_query,
                    'copy_query': None
                }
                table_info.clear()
                table_info.update(ordered_table_info)

        return table_list

    def get_table_row_count(self, db: str, schema: str, table: str) -> int:
        """Get row count for a table."""
        self._ensure_connection()
        
        query = f"SELECT COUNT(*) as count FROM {db}.{schema}.{table}"
        
        try:
            self.cursor.execute(query)
            result = self.cursor.fetchone()
            return result[0] if result else 0
        except Exception as e:
            logger.error(f"Error getting row count: {e}")
            raise SnowflakeQueryError(f"Failed to get row count: {e}")

    def run_cleaning_query(self, table_info: Dict[str, Any], external_stage: str) -> Tuple[bool, Optional[str]]:
        """
        Execute cleaning query for a table to remove existing data from GCS.

        Args:
            table_info: Dictionary containing table info and cleaning_query
            external_stage: Name of the external stage

        Returns:
            Tuple of (success: bool, error_message: Optional[str])

        Raises:
            SnowflakeConnectionError: If no active connection
        """
        self._ensure_connection()
        
        db = table_info['database']
        schema = table_info['schema']
        table = table_info['table']
        cleaning_query = table_info.get('cleaning_query')
        
        if not cleaning_query:
            error_msg = "No cleaning query available for table"
            logger.error(f"{error_msg}: {db}.{schema}.{table}")
            return False, error_msg

        try:
            self._switch_database_if_needed(db)
            
            logger.info(f"Cleaning existing data for {db}.{schema}.{table}...")
            logger.info(f"DEBUG: About to execute cleaning query: {cleaning_query}")
            self.cursor.execute(cleaning_query)
            
            logger.info(f"Successfully cleaned data for {db}.{schema}.{table}")
            return True, None
            
        except (ProgrammingError, SnowflakeQueryError) as e:
            error_msg = f"Cleaning query failed: {e}"
            logger.error(f"Error cleaning data for {db}.{schema}.{table}: {error_msg}")
            return False, error_msg
        except Exception as e:
            error_msg = f"Unexpected error during cleaning: {e}"
            logger.error(f"Unexpected error cleaning data for {db}.{schema}.{table}: {error_msg}")
            return False, error_msg

    def run_copy_query(self, table_info: Dict[str, Any], external_stage: str) -> Tuple[bool, Optional[str]]:
        """
        Execute COPY query for a table.

        Args:
            table_info: Dictionary containing table info and copy_query
            external_stage: Name of the external stage

        Returns:
            Tuple of (success: bool, error_message: Optional[str])

        Raises:
            SnowflakeConnectionError: If no active connection
        """
        self._ensure_connection()
        
        db = table_info['database']
        schema = table_info['schema']
        table = table_info['table']
        copy_query = table_info.get('copy_query')
        
        if not copy_query:
            error_msg = "No copy query available for table"
            logger.error(f"{error_msg}: {db}.{schema}.{table}")
            return False, error_msg

        try:
            self._switch_database_if_needed(db)
            
            logger.info(f"Copying data for {db}.{schema}.{table}...")
            logger.info(f"DEBUG: About to execute copy query: {copy_query}")
            self.cursor.execute(copy_query)
            
            logger.info(f"Successfully copied {db}.{schema}.{table}")
            return True, None
            
        except Exception as e:
            error_msg = f"Copy failed: {e}"
            logger.error(f"{error_msg} for {db}.{schema}.{table}")
            return False, error_msg
