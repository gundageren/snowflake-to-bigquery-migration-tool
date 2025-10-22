import os
from datetime import datetime
from pathlib import Path
from typing import Tuple

from dotenv import load_dotenv

DEFAULT_CONFIG_PATH = 'config/tables.yml'
DEFAULT_CONNECTION_NAME = 'default'
DEFAULT_BQ_LOCATION = 'EU'
DEFAULT_BQ_PREFIX = 'snowflake_'
DEFAULT_LOGS_PATH = 'logs'
DEFAULT_SAMPLE_LIMIT = 100


class MigrationConfig:
    """Configuration management for the migration process."""
    
    def __init__(self) -> None:
        """Load configuration from environment variables."""
        load_dotenv()
        
        self.config_file_path = os.getenv('CONFIG_FILE_PATH', DEFAULT_CONFIG_PATH)
        self.snowflake_connection_name = os.getenv('SNOWFLAKE_CONNECTION_NAME', DEFAULT_CONNECTION_NAME)
        self.external_stage = os.getenv('EXTERNAL_STAGE')
        self.gcp_project_id = os.getenv('PROJECT_ID')
        self.gcs_uri = os.getenv('GCS_URI')
        self.bigquery_data_location = os.getenv('BIGQUERY_DATA_LOCATION', DEFAULT_BQ_LOCATION)
        self.bigquery_dataset_prefix = os.getenv('BIGQUERY_DATASET_PREFIX', DEFAULT_BQ_PREFIX)
        self.logs_path = os.getenv('LOGS_PATH', DEFAULT_LOGS_PATH)
        
        self.dry_run: bool = False
        self.interactive: bool = False
        self.sample: bool = False
        self.verbose: bool = False
        
    def validate(self) -> None:
        """Validate required configuration values."""
        if not self.external_stage:
            raise ValueError("EXTERNAL_STAGE environment variable is required")
        if not self.gcp_project_id:
            raise ValueError("PROJECT_ID environment variable is required")
        if not self.gcs_uri:
            raise ValueError("GCS_URI environment variable is required")

    def _generate_run_id(self) -> str:
        """Generate timestamp-based run ID for log files."""
        return datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    
    def _ensure_logs_dir(self) -> None:
        """Ensure logs directory exists."""
        Path(self.logs_path).mkdir(parents=True, exist_ok=True)
    
    def create_log_files(self) -> Tuple[str, str]:
        """Create migration log file paths with timestamp."""
        run_id = self._generate_run_id()
        self._ensure_logs_dir()
        
        succeeded_file = f"{self.logs_path}/succeeded_tables_{run_id}.yml"
        failed_file = f"{self.logs_path}/failed_tables_{run_id}.yml"
        
        return succeeded_file, failed_file
    
    def create_dry_run_file(self) -> str:
        """Create dry run log file path with timestamp."""
        run_id = self._generate_run_id()
        self._ensure_logs_dir()
        
        return f"{self.logs_path}/dry_mode_{run_id}.yml"
