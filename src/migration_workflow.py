import logging
from typing import List, Dict, Any, Tuple

import yaml

from bigquery.bigquery_client import BigQueryClient
from snowflake.snowflake_client import SnowflakeClient
from config import MigrationConfig
from utils.user_interaction import ask_user_permission_per_table, ask_user_for_retry, format_table_name

logger = logging.getLogger(__name__)

MSG_DRY_RUN = "Dry run mode enabled - showing migration plan without executing queries."
MSG_INTERRUPTED = "Migration process interrupted by user (Ctrl+C)."


def migrate_single_table(
    table_info: Dict[str, Any], 
    sf_client: SnowflakeClient, 
    bq_client: BigQueryClient,
    config: MigrationConfig
) -> Tuple[bool, str]:
    """
    Migrate a single table from Snowflake to BigQuery.

    Args:
        table_info: Table information dictionary
        sf_client: Connected Snowflake client
        bq_client: Connected BigQuery client
        config: Migration configuration
        
    Returns:
        Tuple of (success: bool, error_message: str)
    """
    table_name = format_table_name(table_info)
    logger.info(f"Processing table {table_name}...")
    
    if config.verbose and not config.interactive:
        logger.info(f"\n{'='*60}")
        logger.info(f"Executing migration for: {table_name}")
        logger.info(f"{'='*60}")
        
        if 'cleaning_query' in table_info:
            logger.info(f"\nCleaning Query:")
            logger.info(f"{'-'*40}")
            logger.info(table_info['cleaning_query'])
            logger.info(f"{'-'*40}")
        
        if 'copy_query' in table_info:
            logger.info(f"\nCopy Query:")
            logger.info(f"{'-'*40}")
            logger.info(table_info['copy_query'])
            logger.info(f"{'-'*40}")
        logger.info()
    
    while True:
        success, error_msg = sf_client.run_cleaning_query(table_info, config.external_stage)
        
        if success:
            break
        elif config.interactive:
            action = ask_user_for_retry(table_info, "Snowflake cleaning")
            if action == 'retry':
                continue
            elif action == 'edit':
                from utils.user_interaction import edit_cleaning_query
                if edit_cleaning_query(table_info):
                    continue
                else:
                    return False, f"Snowflake cleaning failed: {error_msg}"
                return False, f"Snowflake cleaning failed: {error_msg}"
        else:
            return False, f"Snowflake cleaning failed: {error_msg}"
    
    while True:
        success, error_msg = sf_client.run_copy_query(table_info, config.external_stage)
        
        if success:
            break
        elif config.interactive:
            action = ask_user_for_retry(table_info, "Snowflake COPY")
            if action == 'retry':
                continue
            elif action == 'edit':
                from utils.user_interaction import edit_copy_query
                if edit_copy_query(table_info):
                    continue
                else:
                    return False, f"Snowflake COPY failed: {error_msg}"
                return False, f"Snowflake COPY failed: {error_msg}"
        else:
            return False, f"Snowflake COPY failed: {error_msg}"
    
    if config.interactive:
        from utils.user_interaction import ask_bigquery_table_permission
        proceed, abort = ask_bigquery_table_permission(table_info)
        
        if abort:
            return False, "BigQuery loading aborted by user"
        elif not proceed:
            return False, "BigQuery loading skipped by user"
    
    sf_count = sf_client.get_table_row_count(
        table_info['database'],
        table_info['schema'],
        table_info['table']
    )

    while True:
        success, error_msg, bq_count = bq_client.create_bq_table(table_info)
        
        if success:
            if sf_count != bq_count:
                error_msg = f"Row count mismatch: Snowflake={sf_count}, BigQuery={bq_count}"
                logger.error(error_msg)
                logger.info(f"ERROR: {error_msg}")
                return False, error_msg
            
            logger.info(f"{sf_count} rows migrated successfully")
            break
        elif config.interactive:
            from utils.user_interaction import ask_bigquery_table_permission
            logger.info(f"\nBigQuery table creation failed: {error_msg}")
            proceed, abort = ask_bigquery_table_permission(table_info)
            
            if abort:
                return False, f"BigQuery table creation failed: {error_msg}"
            elif proceed:
                continue
            else:
                return False, f"BigQuery table creation failed: {error_msg}"
        else:
            return False, f"BigQuery table creation failed: {error_msg}"
    
    logger.info(f"Successfully migrated {table_name}")
    return True, ""


def handle_table_result(
    table_info: Dict[str, Any], 
    success: bool, 
    error_msg: str,
    succeeded_tables: List[Dict], 
    failed_tables: List[Dict]
) -> None:
    """
    Handle the result of a table migration attempt.

    Args:
        table_info: Table information dictionary
        success: Whether migration succeeded
        error_msg: Error message if failed
        succeeded_tables: List to append successful tables
        failed_tables: List to append failed tables
    """
    if success:
        succeeded_tables.append(table_info)
    else:
        table_info['error'] = error_msg
        failed_tables.append(table_info)


def write_dry_run_file(table_list_with_queries: List[Dict[str, Any]], dry_run_file: str) -> None:
    """
    Write dry run table information to a timestamped YAML file.

    Args:
        table_list_with_queries: List of tables with generated queries
        dry_run_file: Full path to the dry run file
    """
    with open(dry_run_file, 'w') as f:
        if table_list_with_queries:
            yaml.dump(table_list_with_queries, f, default_flow_style=False, sort_keys=False, allow_unicode=True)
        else:
            f.write('')
    
    logger.info(f"Dry run analysis saved to '{dry_run_file}' ({len(table_list_with_queries)} table(s))")
    logger.info(f"Table analysis saved to: {dry_run_file}")


def run_dry_mode(table_list_with_queries: List[Dict[str, Any]], config: MigrationConfig) -> None:
    """
    Run in dry mode - show complete table information and queries without executing.
    Also saves the table information to a timestamped file.
    
    Args:
        table_list_with_queries: List of tables with generated COPY queries
        config: Migration configuration (for logs path)
    """
    logger.info(MSG_DRY_RUN)
    logger.info("Tables to be migrated:")
    
    for i, table_info in enumerate(table_list_with_queries, 1):
        table_name = format_table_name(table_info)
        
        logger.info(f"\n{'='*60}")
        logger.info(f"Table {i}/{len(table_list_with_queries)}: {table_name}")
        logger.info(f"{'='*60}")
        logger.info(f"Database: {table_info['database']}")
        logger.info(f"Schema:   {table_info['schema']}")
        logger.info(f"Table:    {table_info['table']}")
        
        if 'table_type' in table_info:
            logger.info(f"Type:     {table_info['table_type']}")
        
        logger.info(f"\nGenerated COPY Query:")
        logger.info(f"{'-'*40}")
        logger.info(table_info['copy_query'])
        logger.info(f"{'-'*40}")
    
    dry_run_file = config.create_dry_run_file()
    write_dry_run_file(table_list_with_queries, dry_run_file)
    
    logger.info(f"\nDry run complete! Found {len(table_list_with_queries)} table(s) ready for migration.")


def run_migration_workflow(
    table_list: List[Dict[str, Any]], 
    sf_client: SnowflakeClient,
    bq_client: BigQueryClient, 
    config: MigrationConfig
) -> Tuple[List[Dict], List[Dict]]:
    """
    Execute the full migration workflow for all tables.
    
    Args:
        table_list: List of tables with COPY queries
        sf_client: Connected Snowflake client
        bq_client: Connected BigQuery client
        config: Migration configuration
        
    Returns:
        Tuple of (succeeded_tables, failed_tables)
    """
    succeeded_tables = []
    failed_tables = []

    try:
        for i, table_info in enumerate(table_list):
            if config.interactive:
                proceed, abort = ask_user_permission_per_table(table_info, config.verbose)
                
                if abort:
                    for remaining_table in table_list[i:]:
                        handle_table_result(remaining_table, False, 'Aborted by user', 
                                          succeeded_tables, failed_tables)
                    break
                    
                if not proceed:
                    handle_table_result(table_info, False, 'Skipped by user',
                                      succeeded_tables, failed_tables)
                    continue
            
            success, error_msg = migrate_single_table(table_info, sf_client, bq_client, config)
            handle_table_result(table_info, success, error_msg, succeeded_tables, failed_tables)

            logger.info("---------------------------------------------------------------------------------------------")
                
    except KeyboardInterrupt:
        logger.warning(MSG_INTERRUPTED)
        for remaining_table in table_list[i:]:
            if remaining_table not in succeeded_tables and remaining_table not in failed_tables:
                handle_table_result(remaining_table, False, 'Aborted by user (Ctrl+C)',
                                  succeeded_tables, failed_tables)
    
    return succeeded_tables, failed_tables

