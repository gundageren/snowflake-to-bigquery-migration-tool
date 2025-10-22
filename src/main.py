import argparse
import atexit
import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Tuple

import yaml

from bigquery.bigquery_client import BigQueryClient
from snowflake.snowflake_client import SnowflakeClient
from config import MigrationConfig, DEFAULT_SAMPLE_LIMIT
from utils.user_interaction import ask_user_permission_per_table, ask_user_for_retry, format_table_name
from utils.file_utils import log_table_counts, write_results_on_exit
from migration_workflow import run_dry_mode, run_migration_workflow

MSG_TABLES_RETURNED = "No tables were returned. Exiting."
MSG_DRY_RUN = "Dry run mode enabled - showing migration plan without executing queries."
MSG_INTERRUPTED = "Migration process interrupted by user (Ctrl+C)."

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

logging.getLogger('snowflake').setLevel(logging.WARNING)
logging.getLogger('google.cloud.bigquery').setLevel(logging.WARNING)


def main(dry_run: bool, interactive: bool, sample: bool, verbose: bool = False) -> None:
    """
    Main function to orchestrate the migration process.
    
    Args:
        dry_run: Whether to run in dry mode (show queries only)
        interactive: Whether to ask for user permission per table
        sample: Whether to use sample data only
        verbose: Whether to show cleaning and copy queries during execution
    """
    try:
        config = MigrationConfig()
        config.dry_run = dry_run
        config.interactive = interactive
        config.sample = sample
        config.verbose = verbose
        config.validate()
        
        if not dry_run:
            succeeded_file, failed_file = config.create_log_files()
            succeeded_tables = []
            failed_tables = []
            
            atexit.register(write_results_on_exit, succeeded_tables, failed_tables, 
                           succeeded_file, failed_file)
        
        with SnowflakeClient(connection_name=config.snowflake_connection_name) as sf, \
             BigQueryClient(project_id=config.gcp_project_id, 
                           gcs_uri=config.gcs_uri,
                           location=config.bigquery_data_location,
                           dataset_prefix=config.bigquery_dataset_prefix) as bq:
            
            table_list = sf.list_tables_from_yaml(file_path=config.config_file_path)

            if not table_list:
                logger.warning(MSG_TABLES_RETURNED)
                return

            log_table_counts(table_list)
            table_list_with_queries = sf.generate_copy_queries(
                table_list, config.external_stage, sample=sample
            )
            
            if dry_run:
                run_dry_mode(table_list_with_queries, config)
            else:
                succeeded, failed = run_migration_workflow(
                    table_list_with_queries, sf, bq, config
                )
                succeeded_tables.extend(succeeded)
                failed_tables.extend(failed)
                
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        raise


def parse_arguments() -> argparse.Namespace:
    """
    Parse command line arguments.
    
    Returns:
        argparse.Namespace: Parsed command line arguments
    """
    parser = argparse.ArgumentParser(
        description="Migrate data from Snowflake to BigQuery via GCS",
        epilog="Example: python main.py --dry_run --interactive --sample"
    )
    
    parser.add_argument(
        "--dry_run", 
        action="store_true", 
        help="Show complete table information and queries without executing them"
    )
    parser.add_argument(
        "--interactive", 
        action="store_true",
        help="Ask for permission before processing each table"  
    )
    parser.add_argument(
        "--sample", 
        action="store_true",
        help=f"Copy only a sample of {DEFAULT_SAMPLE_LIMIT} rows per table"
    )
    parser.add_argument(
        "--verbose", 
        action="store_true",
        help="Show cleaning and copy queries during execution in all modes"
    )
    
    return parser.parse_args()


if __name__ == "__main__":
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    os.chdir(project_root)
    
    args = parse_arguments()
    
    logger.info(f"Starting migration (dry_run={args.dry_run}, "
               f"interactive={args.interactive}, sample={args.sample})")
    
    main(dry_run=args.dry_run, interactive=args.interactive, sample=args.sample, verbose=args.verbose)
