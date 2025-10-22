import logging
import os
import re
from typing import Optional, Dict, Any, Tuple, List

from google.cloud import bigquery
from google.cloud.exceptions import GoogleCloudError

logger = logging.getLogger(__name__)

DEFAULT_DATASET_PREFIX = 'snowflake_'
DEFAULT_LOCATION = 'EU'
PARQUET_FORMAT = bigquery.SourceFormat.PARQUET
WRITE_TRUNCATE = bigquery.WriteDisposition.WRITE_TRUNCATE

SNOWFLAKE_TO_BIGQUERY_TYPES = {
    'NUMBER': 'NUMERIC',
    'DECIMAL': 'NUMERIC', 
    'NUMERIC': 'NUMERIC',
    'INT': 'INTEGER',
    'INTEGER': 'INTEGER',
    'BIGINT': 'INTEGER',
    'SMALLINT': 'INTEGER',
    'TINYINT': 'INTEGER',
    'BYTEINT': 'INTEGER',
    'FLOAT': 'FLOAT64',
    'FLOAT4': 'FLOAT64',
    'FLOAT8': 'FLOAT64',
    'DOUBLE': 'FLOAT64',
    'DOUBLE_PRECISION': 'FLOAT64',
    'REAL': 'FLOAT64',
    
    'VARCHAR': 'STRING',
    'CHAR': 'STRING',
    'CHARACTER': 'STRING',
    'STRING': 'STRING',
    'TEXT': 'STRING',
    'BINARY': 'BYTES',
    'VARBINARY': 'BYTES',
    
    'BOOLEAN': 'BOOLEAN',
    
    'DATE': 'DATE',
    'DATETIME': 'DATETIME',
    'TIME': 'TIME',
    'TIMESTAMP': 'TIMESTAMP',
    'TIMESTAMP_LTZ': 'TIMESTAMP',
    'TIMESTAMP_NTZ': 'TIMESTAMP',
    'TIMESTAMP_TZ': 'TIMESTAMP',
    
    'VARIANT': 'JSON',
    'OBJECT': 'JSON',
    'ARRAY': 'JSON',
    
    'GEOGRAPHY': 'GEOGRAPHY',
}


class BigQueryConnectionError(Exception):
    """Raised when BigQuery connection fails."""
    pass


class BigQueryDatasetError(Exception):
    """Raised when BigQuery dataset operations fail."""
    pass


class BigQueryTableError(Exception):
    """Raised when BigQuery table operations fail."""
    pass


class BigQueryClient:
    """
    Manages BigQuery connections and data loading operations.
    
    Provides functionality to:
    - Connect to BigQuery using GCP credentials
    - Create datasets and tables from GCS Parquet files
    - Handle both managed and external tables
    - Manage table loading jobs
    """

    def __init__(
        self, 
        project_id: str, 
        gcs_uri: str, 
        location: str = DEFAULT_LOCATION,
        gcp_connection_file_path: Optional[str] = None,
        dataset_prefix: str = DEFAULT_DATASET_PREFIX
    ):
        """
        Initialize BigQuery client.

        Args:
            project_id: Google Cloud project ID
            gcs_uri: Base GCS URI where data is stored (e.g., 'gs://my-bucket/')
            location: BigQuery dataset location (e.g., 'EU', 'US')
            gcp_connection_file_path: Path to GCP service account JSON file
            dataset_prefix: Prefix for BigQuery dataset names
        """
        self.project_id = project_id
        self.gcs_uri = gcs_uri.rstrip('/')
        self.location = location
        self.gcp_connection_file_path = gcp_connection_file_path
        self.dataset_prefix = dataset_prefix
        self.client: Optional[bigquery.Client] = None

    def __enter__(self):
        """Connect to BigQuery when entering context manager."""
        try:
            if self.gcp_connection_file_path:
                os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = self.gcp_connection_file_path
            
            self.client = bigquery.Client(project=self.project_id, location=self.location)
            logger.info("BigQuery client initialized successfully.")
            return self
            
        except GoogleCloudError as e:
            logger.error(f"BigQuery connection failed: {e}")
            raise BigQueryConnectionError(f"Failed to connect to project '{self.project_id}': {e}")
        except Exception as e:
            logger.error(f"Unexpected error during BigQuery connection: {e}")
            raise BigQueryConnectionError(f"Unexpected connection error: {e}")

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Close BigQuery connection when exiting context manager."""
        if self.client:
            try:
                self.client.close()
                logger.debug("BigQuery client closed.")
            except Exception as e:
                logger.warning(f"Error closing BigQuery client: {e}")
        self.client = None
        return False

    def _ensure_client(self) -> None:
        """Ensure we have an active BigQuery client."""
        if not self.client:
            raise BigQueryConnectionError("No active BigQuery client")

    def _build_dataset_id(self, database: str, schema: str) -> str:
        """Build BigQuery dataset ID from database and schema names."""
        dataset_id = f"{self.dataset_prefix}{database}_{schema}".replace('-', '_').lower()
        return dataset_id

    def _build_gcs_source_uri(self, database: str, schema: str, table: str) -> str:
        """Build GCS source URI for table data."""
        return f"{self.gcs_uri}/{database.lower()}/{schema.lower()}/{table.lower()}/*"

    def _create_dataset_if_needed(self, dataset_id: str) -> None:
        """Create dataset if it doesn't exist."""
        try:
            dataset_ref = self.client.dataset(dataset_id)
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = self.location
            
            self.client.create_dataset(dataset, exists_ok=True)
            logger.debug(f"Ensured dataset {dataset_id} exists in location {self.location}")
            
        except GoogleCloudError as e:
            error_msg = f"Failed to create dataset {dataset_id}: {e}"
            logger.error(error_msg)
            raise BigQueryDatasetError(error_msg)
        except Exception as e:
            error_msg = f"Unexpected error creating dataset {dataset_id}: {e}"
            logger.error(error_msg)
            raise BigQueryDatasetError(error_msg)

    def _create_load_job_config(self, custom_schema: Optional[str] = None) -> bigquery.LoadJobConfig:
        """Create configuration for BigQuery load job."""
        job_config = bigquery.LoadJobConfig(
            source_format=PARQUET_FORMAT,
            write_disposition=WRITE_TRUNCATE,
            column_name_character_map="V2"
        )
        
        if custom_schema:
            import json
            schema_data = json.loads(custom_schema)
            schema_fields = []
            for field in schema_data:
                schema_fields.append(bigquery.SchemaField(
                    field['name'], 
                    field['type'], 
                    field.get('mode', 'NULLABLE')
                ))
            job_config.schema = schema_fields
        else:
            job_config.autodetect = True
            
        return job_config



    def _create_table_with_options(self, table_ref: bigquery.TableReference, bq_options: Dict[str, Any]) -> None:
        """Create an empty table with partitioning and clustering options."""
        table = bigquery.Table(table_ref)
        
        if 'custom_schema' in bq_options:
            import json
            schema_data = json.loads(bq_options['custom_schema'])
            schema_fields = []
            for field in schema_data:
                schema_fields.append(bigquery.SchemaField(
                    field['name'], 
                    field['type'], 
                    field.get('mode', 'NULLABLE')
                ))
            table.schema = schema_fields
        
        if 'partition_field' in bq_options:
            partition_field = bq_options['partition_field']
            partition_type = bq_options.get('partition_type', 'DAY')
            
            if partition_type == 'DAY':
                table.time_partitioning = bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY,
                    field=partition_field
                )
            elif partition_type == 'HOUR':
                table.time_partitioning = bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.HOUR,
                    field=partition_field
                )
            elif partition_type == 'MONTH':
                table.time_partitioning = bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.MONTH,
                    field=partition_field
                )
            elif partition_type == 'YEAR':
                table.time_partitioning = bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.YEAR,
                    field=partition_field
                )
            
            logger.info(f"Will create table with {partition_type} partitioning on field '{partition_field}'")
        
        if 'cluster_fields' in bq_options:
            cluster_fields = bq_options['cluster_fields']
            table.clustering_fields = cluster_fields
            logger.info(f"Will create table with clustering on fields: {', '.join(cluster_fields)}")
        
        self.client.create_table(table)
        logger.info(f"Created table {table_ref.dataset_id}.{table_ref.table_id} with specified options")

    def get_table_row_count(self, dataset_id: str, table_id: str) -> int:
        """Get row count from BigQuery table."""
        query = f"SELECT COUNT(*) as count FROM `{self.project_id}.{dataset_id}.{table_id}`"
        
        try:
            result = self.client.query(query).result()
            return next(iter(result)).get('count')
        except Exception as e:
            if "404 Not found: Table" in str(e):
                return 0
            raise

    def create_bq_table(self, table_info: Dict[str, Any]) -> Tuple[bool, Optional[str], int]:
        """
        Load Parquet files from GCS into a BigQuery table.

        Args:
            table_info: Dictionary containing table information (database, schema, table)
                       May also contain BigQuery options: custom_schema, partition_field, 
                       partition_type, cluster_fields

        Returns:
            Tuple of (success: bool, error_message: Optional[str], row_count: int)
        """
        self._ensure_client()
        
        database = table_info['database']
        schema = table_info['schema']
        table = table_info['table']
        
        bq_options = {}
        for key in ['custom_schema', 'partition_field', 'partition_type', 'cluster_fields']:
            if key in table_info:
                bq_options[key] = table_info[key]
        
        try:
            dataset_id = self._build_dataset_id(database, schema)
            self._create_dataset_if_needed(dataset_id)
            
            table_id = table.lower()
            table_ref = self.client.dataset(dataset_id).table(table_id)
            source_uri = self._build_gcs_source_uri(database, schema, table)
            
            has_custom_schema = 'custom_schema' in bq_options
            has_partition_or_cluster = 'partition_field' in bq_options or 'cluster_fields' in bq_options
            
            if bq_options:
                try:
                    self.client.delete_table(table_ref, not_found_ok=True)
                    logger.info(f"Deleted existing table {dataset_id}.{table_id}")
                except Exception as e:
                    logger.debug(f"Table {dataset_id}.{table_id} did not exist or could not be deleted: {e}")
                
                if has_custom_schema:
                    self._create_table_with_options(table_ref, bq_options)
            
            logger.info(f"Loading {database}.{schema}.{table} from GCS...")
            
            job_config = bigquery.LoadJobConfig(
                source_format=PARQUET_FORMAT,
                write_disposition=WRITE_TRUNCATE,
                column_name_character_map="V2"
            )
            
            if has_custom_schema:
                import json
                schema_data = json.loads(bq_options['custom_schema'])
                schema_fields = []
                for field in schema_data:
                    schema_fields.append(bigquery.SchemaField(
                        field['name'], 
                        field['type'], 
                        field.get('mode', 'NULLABLE')
                    ))
                job_config.schema = schema_fields
            else:
                job_config.autodetect = True
                
                if has_partition_or_cluster:
                    if 'partition_field' in bq_options:
                        partition_type = bq_options.get('partition_type', 'DAY')
                        job_config.time_partitioning = bigquery.TimePartitioning(
                            type_=getattr(bigquery.TimePartitioningType, partition_type),
                            field=bq_options['partition_field']
                        )
                    
                    if 'cluster_fields' in bq_options:
                        job_config.clustering_fields = bq_options['cluster_fields']
            
            load_job = self.client.load_table_from_uri(source_uri, table_ref, job_config=job_config)
            load_job.result()
            
            destination_table = self.client.get_table(table_ref)
            row_count = destination_table.num_rows
            
            logger.info(f"Successfully loaded {row_count} rows into {dataset_id}.{table_id}")
            return True, None, row_count
            
        except (BigQueryDatasetError, BigQueryTableError) as e:
            return False, str(e), 0
        except GoogleCloudError as e:
            error_msg = f"BigQuery error loading {database}.{schema}.{table}: {e}"
            logger.error(error_msg)
            return False, error_msg, 0
        except Exception as e:
            error_msg = f"Unexpected error loading {database}.{schema}.{table}: {e}"
            logger.error(error_msg)
            return False, error_msg, 0

    def _create_external_config(self, source_uri: str) -> bigquery.ExternalConfig:
        """Create external configuration for BigQuery external table."""
        external_config = bigquery.ExternalConfig("PARQUET")
        external_config.source_uris = [source_uri]
        external_config.autodetect = True
        return external_config

    def _delete_table_if_exists(self, table_ref: bigquery.TableReference) -> None:
        """Delete table if it exists, otherwise do nothing."""
        try:
            self.client.get_table(table_ref)
            self.client.delete_table(table_ref)
            logger.debug(f"Deleted existing table {table_ref.path}")
        except Exception:
            logger.debug(f"Table {table_ref.path} does not exist, skipping deletion")

    def create_bq_external_table(self, table_info: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        """
        Create BigQuery external table pointing to GCS Parquet files.

        Args:
            table_info: Dictionary containing table information (database, schema, table)

        Returns:
            Tuple of (success: bool, error_message: Optional[str])
        """
        self._ensure_client()
        
        database = table_info['database']
        schema = table_info['schema']
        table = table_info['table']
        
        try:
            dataset_id = self._build_dataset_id(database, schema)
            self._create_dataset_if_needed(dataset_id)
            
            table_name = f"{table}_external".lower()
            table_ref = self.client.dataset(dataset_id).table(table_name)
            source_uri = self._build_gcs_source_uri(database, schema, table)
            
            self._delete_table_if_exists(table_ref)
            
            external_config = self._create_external_config(source_uri)
            
            external_table = bigquery.Table(table_ref)
            external_table.external_data_configuration = external_config
            
            self.client.create_table(external_table, exists_ok=True)
            
            logger.info(f"Created external table {dataset_id}.{table_name} → {source_uri}")
            return True, None
            
        except (BigQueryDatasetError, BigQueryTableError) as e:
            return False, str(e)
        except GoogleCloudError as e:
            error_msg = f"BigQuery error creating external table for {database}.{schema}.{table}: {e}"
            logger.error(error_msg)
            return False, error_msg
        except Exception as e:
            error_msg = f"Unexpected error creating external table for {database}.{schema}.{table}: {e}"
            logger.error(error_msg)
            return False, error_msg

    def _convert_snowflake_type_to_bigquery(self, snowflake_type: str) -> str:
        """
        Convert Snowflake data type to BigQuery data type.
        
        Args:
            snowflake_type: Snowflake data type (e.g., 'VARCHAR(255)', 'NUMBER(10,2)')
            
        Returns:
            str: Corresponding BigQuery data type
        """
        base_type = snowflake_type.upper().split('(')[0].strip()
        
        bigquery_type = SNOWFLAKE_TO_BIGQUERY_TYPES.get(base_type, 'STRING')
        
        logger.debug(f"Converted Snowflake type '{snowflake_type}' -> BigQuery type '{bigquery_type}'")
        return bigquery_type

    def _extract_column_aliases_from_copy_query(self, copy_query: str) -> List[str]:
        """
        Extract column aliases from a COPY query's SELECT statement.
        
        Args:
            copy_query: The COPY query containing SELECT with AS clauses
            
        Returns:
            List[str]: List of column aliases as they appear in the query
        """
        if not copy_query:
            logger.warning("Empty copy_query provided")
            return []
        
        try:
            select_match = re.search(r'SELECT\s+(.*?)\s+FROM', copy_query, re.IGNORECASE | re.DOTALL)
            if not select_match:
                logger.warning("Could not find SELECT statement in copy_query")
                return []
            
            select_part = select_match.group(1)
            
            alias_pattern = r'AS\s+"([^"]*)"'
            aliases = re.findall(alias_pattern, select_part, re.IGNORECASE)
            
            logger.debug(f"Extracted {len(aliases)} column aliases from copy_query")
            return aliases
            
        except Exception as e:
            logger.error(f"Error extracting column aliases from copy_query: {e}")
            return []

    def _normalize_column_name_v2(self, column_name: str) -> str:
        """
        Normalize column name using BigQuery V2 character mapping rules.
        This matches the behavior of column_name_character_map="V2" in BigQuery load jobs.
        
        Args:
            column_name: Original column name
            
        Returns:
            str: Normalized column name that follows BigQuery naming conventions
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
            
        # Avoid reserved prefixes by adding suffix if needed
        reserved_prefixes = ['_TABLE_', '_FILE_', '_PARTITION_']
        for prefix in reserved_prefixes:
            if normalized.upper().startswith(prefix):
                normalized = normalized + '_'
                break

        normalized = normalized.lower()
                
        return normalized

    def infer_schema_from_table_info(self, table_info: Dict[str, Any]) -> List[bigquery.SchemaField]:
        """
        Infer BigQuery schema from table info containing columns and copy_query.
        
        Args:
            table_info: Dictionary containing 'columns' list and 'copy_query' string
            
        Returns:
            List[bigquery.SchemaField]: Inferred schema fields with deduplicated names
        """
        columns = table_info.get('columns', [])
        copy_query = table_info.get('copy_query', '')
        
        if not columns:
            logger.warning("No columns found in table_info")
            return []
        
        # Extract column aliases from copy query
        aliases = self._extract_column_aliases_from_copy_query(copy_query)
        
        schema_fields = []
        used_names = set()  # Track used names to handle duplicates
        
        for i, column in enumerate(columns):
            column_name = column.get('column_name', f'column_{i}')
            data_type = column.get('data_type', 'STRING')
            
            if i < len(aliases):
                base_field_name = self._normalize_column_name_v2(aliases[i])
            else:
                base_field_name = self._normalize_column_name_v2(column_name)
            
            # Handle duplicate column names by appending numbers
            field_name = base_field_name
            counter = 2
            while field_name in used_names:
                if base_field_name == "_":
                    field_name = f"_{counter}"
                else:
                    field_name = f"{base_field_name}_{counter}"
                counter += 1
            
            used_names.add(field_name)
            bigquery_type = self._convert_snowflake_type_to_bigquery(data_type)
            
            schema_field = bigquery.SchemaField(
                name=field_name,
                field_type=bigquery_type,
                mode='NULLABLE'
            )
            
            schema_fields.append(schema_field)
        
        original_count = len(columns)
        unique_count = len(used_names)
        if original_count != unique_count:
            logger.warning(f"Handled {original_count - unique_count} duplicate column names during schema inference")
        
        logger.info(f"Inferred schema with {len(schema_fields)} fields")
        return schema_fields
