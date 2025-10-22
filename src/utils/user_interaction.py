"""User interaction utilities for the migration tool."""

import json
import logging
import tempfile
import subprocess
import os
from typing import Dict, Any, Tuple, List

import inquirer

PROCEED_RESPONSES = ['yes', 'y']
SKIP_RESPONSES = ['next']
ABORT_RESPONSES = ['abort']
RETRY_RESPONSES = ['retry']
EDIT_RESPONSES = ['edit']
EDIT_CLEANING_RESPONSES = ['edit-cleaning', 'ec']
EDIT_COPY_RESPONSES = ['edit-copy', 'et']
EDIT_SCHEMA_RESPONSES = ['edit-schema', 'es']
EDIT_PARTITION_RESPONSES = ['edit-partition-key', 'ep']
EDIT_CLUSTER_RESPONSES = ['edit-cluster-keys', 'ecl']

logger = logging.getLogger(__name__)


def format_table_name(table_info: Dict[str, Any]) -> str:
    """
    Format table name for display.
    
    Args:
        table_info: Dictionary containing table information
        
    Returns:
        str: Formatted table name as database.schema.table
    """
    return f"{table_info['database']}.{table_info['schema']}.{table_info['table']}"


def _generate_inferred_schema_json(table_info: Dict[str, Any]) -> str:
    """
    Generate inferred schema as JSON string from table info.
    
    Args:
        table_info: Dictionary containing table information
        
    Returns:
        str: JSON representation of inferred schema
    """
    try:
        # Import here to avoid circular imports
        from bigquery.bigquery_client import BigQueryClient
        
        bq_client = BigQueryClient('dummy-project', 'gs://dummy-bucket')
        
        schema_fields = bq_client.infer_schema_from_table_info(table_info)
        
        schema_json = []
        for field in schema_fields:
            schema_json.append({
                "name": field.name,
                "type": field.field_type,
                "mode": field.mode
            })
        
        return json.dumps(schema_json, indent=2)
        
    except Exception as e:
        logger.error(f"Error generating inferred schema: {e}")
        return ""


def _get_partition_eligible_columns(table_info: Dict[str, Any]) -> List[Dict[str, str]]:
    """
    Get columns eligible for partitioning (DATE, DATETIME, TIMESTAMP types only).
    
    Args:
        table_info: Dictionary containing table information
        
    Returns:
        List of dictionaries with column info suitable for partitioning
    """
    try:
        from bigquery.bigquery_client import BigQueryClient
        
        bq_client = BigQueryClient('dummy-project', 'gs://dummy-bucket')
        schema_fields = bq_client.infer_schema_from_table_info(table_info)
        
        eligible_columns = []
        partition_types = {'DATE', 'DATETIME', 'TIMESTAMP'}
        
        for field in schema_fields:
            if field.field_type in partition_types:
                eligible_columns.append({
                    'name': field.name,
                    'type': field.field_type,
                    'display': f"{field.name} ({field.field_type})"
                })
        
        return eligible_columns
        
    except Exception as e:
        logger.error(f"Error getting partition eligible columns: {e}")
        return []


def _get_available_columns(table_info: Dict[str, Any]) -> List[Dict[str, str]]:
    """
    Get all available columns with their BigQuery types.
    
    Args:
        table_info: Dictionary containing table information
        
    Returns:
        List of dictionaries with column info
    """
    try:
        from bigquery.bigquery_client import BigQueryClient
        
        bq_client = BigQueryClient('dummy-project', 'gs://dummy-bucket')
        schema_fields = bq_client.infer_schema_from_table_info(table_info)
        
        columns = []
        for field in schema_fields:
            columns.append({
                'name': field.name,
                'type': field.field_type,
                'display': f"{field.name} ({field.field_type})"
            })
        
        return columns
        
    except Exception as e:
        logger.error(f"Error getting available columns: {e}")
        return []


def ask_user_permission_per_table(table_info: Dict[str, Any], verbose: bool = False) -> Tuple[bool, bool]:
    """
    Ask user for permission to proceed with table migration.

    Args:
        table_info: Dictionary containing table information
        verbose: Whether to show queries

    Returns:
        Tuple of (proceed_with_migration: bool, abort_all: bool)
    """
    table_name = format_table_name(table_info)
    
    # Show queries if verbose mode is enabled
    if verbose:
        logger.info(f"\n{'='*60}")
        logger.info(f"Table: {table_name}")
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
    
    # Build choices - edit options are always available now
    choices = [
        ('Proceed with migration', 'proceed'),
        ('Skip to next table', 'skip'),
        ('Edit cleaning query', 'edit-cleaning'),
        ('Edit copy query', 'edit-copy'),
        ('Abort migration', 'abort')
    ]
    
    while True:
        try:
            questions = [
                inquirer.List('action',
                            message=f"What would you like to do with {table_name}?",
                            choices=choices,
                            carousel=True)
            ]
            
            answers = inquirer.prompt(questions)
            if not answers:  # User pressed Ctrl+C
                return False, True
                
            action = answers['action']
            
            # Handle edit actions
            if action == 'edit-cleaning':
                if edit_cleaning_query(table_info):
                    logger.info(f"\nCleaning query updated for {table_name}")
                    if verbose:
                        logger.info(f"{'-'*40}")
                        logger.info(table_info['cleaning_query'])
                        logger.info(f"{'-'*40}")
                continue
            elif action == 'edit-copy':
                if edit_copy_query(table_info):
                    logger.info(f"\nCopy query updated for {table_name}")
                    if verbose:
                        logger.info(f"{'-'*40}")
                        logger.info(table_info['copy_query'])
                        logger.info(f"{'-'*40}")
                continue
            elif action == 'proceed':
                return True, False
            elif action == 'skip':
                logger.info(f"Migration for {table_name} skipped by user. Continuing to next table.")
                return False, False
            elif action == 'abort':
                logger.info(f"Migration for {table_name} aborted by user. Skipping all remaining tables.")
                return False, True
                
        except KeyboardInterrupt:
            logger.info("\nMigration aborted by user.")
            return False, True
        except Exception as e:
            logger.error(f"Error in user prompt: {e}")
            return False, True


def edit_cleaning_query(table_info: Dict[str, Any]) -> bool:
    """
    Allow user to edit the cleaning query for a table using an external editor.

    Args:
        table_info: Dictionary containing table information (will be modified)

    Returns:
        bool: True if query was edited and should be retried, False to skip
    """
    current_cleaning_query = table_info.get('cleaning_query', '')
    table_name = format_table_name(table_info)
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as tmp_file:
        tmp_file.write(current_cleaning_query)
        tmp_file_path = tmp_file.name
    
    try:
        editor = os.environ.get('EDITOR')
        
        if not editor:
            if os.name == 'nt':  # Windows
                editor = 'notepad'
            else:  # Unix-like (Linux, macOS)
                for candidate in ['code', 'vim', 'nano', 'vi']:
                    if subprocess.run(['which', candidate], capture_output=True).returncode == 0:
                        editor = candidate
                        break
                else:
                    editor = 'vi'  # Last resort
        
        logger.info(f"\nOpening cleaning query for {table_name} in {editor}...")
        logger.info("Edit the CLEANING query as needed.")
        logger.info("Save and exit when done, or exit without saving to cancel.")
        
        result = subprocess.run([editor, tmp_file_path])
        
        if result.returncode == 0:
            with open(tmp_file_path, 'r') as f:
                edited_query = f.read().strip()
            
            if edited_query != current_cleaning_query and edited_query:
                table_info['cleaning_query'] = edited_query
                logger.info(f"Cleaning query updated for {table_name}")
                logger.info("Cleaning query updated successfully!")
                logger.info(f"DEBUG: New cleaning query stored: {edited_query}")
                return True
            elif not edited_query:
                logger.info("Empty query detected. Skipping...")
                return False
            else:
                logger.info("No changes made to query. Retrying with original query...")
                return True
        else:
            logger.info("Editor exited with error. Skipping...")
            return False
            
    except FileNotFoundError:
        logger.info(f"Editor '{editor}' not found. Available options:")
        logger.info("- Set EDITOR environment variable to your preferred editor")
        logger.info("- Install one of: code, vim, nano")
        logger.info("Skipping table.")
        return False
    except Exception as e:
        logger.info(f"Error opening editor: {e}")
        logger.info("Skipping table.")
        return False
        
    finally:
        try:
            os.unlink(tmp_file_path)
        except OSError:
            pass


def edit_copy_query(table_info: Dict[str, Any]) -> bool:
    """
    Allow user to edit the copy query for a table using an external editor.

    Args:
        table_info: Dictionary containing table information (will be modified)

    Returns:
        bool: True if query was edited and should be retried, False to skip
    """
    current_copy_query = table_info.get('copy_query', '')
    table_name = format_table_name(table_info)
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as tmp_file:
        tmp_file.write(current_copy_query)
        tmp_file_path = tmp_file.name
    
    try:
        editor = os.environ.get('EDITOR')
        
        if not editor:
            if os.name == 'nt':  # Windows
                editor = 'notepad'
            else:  # Unix-like (Linux, macOS)
                for candidate in ['code', 'vim', 'nano', 'vi']:
                    if subprocess.run(['which', candidate], capture_output=True).returncode == 0:
                        editor = candidate
                        break
                else:
                    editor = 'vi'  # Last resort
        
        logger.info(f"\nOpening copy query for {table_name} in {editor}...")
        logger.info("Edit the COPY query as needed.")
        logger.info("Save and exit when done, or exit without saving to cancel.")
        
        result = subprocess.run([editor, tmp_file_path])
        
        if result.returncode == 0:
            with open(tmp_file_path, 'r') as f:
                edited_query = f.read().strip()
            
            if edited_query != current_copy_query and edited_query:
                table_info['copy_query'] = edited_query
                logger.info(f"Copy query updated for {table_name}")
                logger.info("Copy query updated successfully!")
                logger.info(f"DEBUG: New copy query stored: {edited_query}")
                return True
            elif not edited_query:
                logger.info("Empty query detected. Skipping...")
                return False
            else:
                logger.info("No changes made to query. Retrying with original query...")
                return True
        else:
            logger.info("Editor exited with error. Skipping...")
            return False
            
    except FileNotFoundError:
        logger.info(f"Editor '{editor}' not found. Available options:")
        logger.info("- Set EDITOR environment variable to your preferred editor")
        logger.info("- Install one of: code, vim, nano")
        logger.info("Skipping table.")
        return False
    except Exception as e:
        logger.info(f"Error opening editor: {e}")
        logger.info("Skipping table.")
        return False
        
    finally:
        try:
            os.unlink(tmp_file_path)
        except OSError:
            pass


def ask_user_for_retry(table_info: Dict[str, Any], step_name: str) -> str:
    """
    Ask user whether to retry a failed step, with edit option for copy queries.

    Args:
        table_info: Dictionary containing table information
        step_name: Name of the failed step

    Returns:
        str: 'retry', 'edit', or 'skip'
    """
    table_name = format_table_name(table_info)
    
    choices = [('Retry', 'retry'), ('Skip to next table', 'skip')]
    
    if "cleaning" in step_name or "COPY" in step_name:
        choices.insert(1, ('Edit query and retry', 'edit'))
    
    try:
        questions = [
            inquirer.List('action',
                        message=f"{step_name} failed for {table_name}. What would you like to do?",
                        choices=choices,
                        carousel=True)
        ]
        
        answers = inquirer.prompt(questions)
        if not answers:  # User pressed Ctrl+C
            return 'skip'
            
        return answers['action']
        
    except KeyboardInterrupt:
        logger.info("\nSkipping to next table.")
        return 'skip'
    except Exception as e:
        logger.error(f"Error in retry prompt: {e}")
        return 'skip'


def ask_bigquery_table_permission(table_info: Dict[str, Any]) -> Tuple[bool, bool]:
    """
    Ask user for permission to proceed with BigQuery table loading.
    
    Args:
        table_info: Dictionary containing table information
    
    Returns:
        Tuple of (proceed_with_loading: bool, abort_all: bool)
    """
    table_name = format_table_name(table_info)
    
    bq_options = []
    if table_info.get('custom_schema'):
        bq_options.append("Custom schema")
    if table_info.get('partition_field'):
        partition_type = table_info.get('partition_type', 'DAY')
        bq_options.append(f"Partitioned by {table_info['partition_field']} ({partition_type})")
    if table_info.get('cluster_fields'):
        bq_options.append(f"Clustered by {', '.join(table_info['cluster_fields'])}")
    
    if bq_options:
        logger.info(f"\nBigQuery settings: {'; '.join(bq_options)}")
    else:
        logger.info(f"\nBigQuery settings: Auto-detect schema, no partitioning/clustering")
    
    if not table_info.get('custom_schema') and (table_info.get('partition_field') or table_info.get('cluster_fields')):
        logger.info("Note: Custom schema required for partitioning/clustering options")
    
    choices = [
        ('Proceed with loading', 'proceed'),
        ('Skip to next table', 'skip'),
        ('Edit schema', 'edit-schema'),
        ('Edit partition settings', 'edit-partition'),
        ('Edit cluster settings', 'edit-cluster'),
        ('Abort migration', 'abort')
    ]
    
    while True:
        try:
            questions = [
                inquirer.List('action',
                            message=f"BigQuery table loading for {table_name}",
                            choices=choices,
                            carousel=True)
            ]
            
            answers = inquirer.prompt(questions)
            if not answers:  # User pressed Ctrl+C
                return False, True
                
            action = answers['action']
            
            # Handle edit actions
            if action == 'edit-schema':
                if edit_schema(table_info):
                    logger.info(f"\nSchema settings updated for {table_name}")
                continue
            elif action == 'edit-partition':
                if edit_partition_key(table_info):
                    logger.info(f"\nPartitioning settings updated for {table_name}")
                continue
            elif action == 'edit-cluster':
                if edit_cluster_keys(table_info):
                    logger.info(f"\nClustering settings updated for {table_name}")
                continue
            elif action == 'proceed':
                return True, False
            elif action == 'skip':
                logger.info(f"BigQuery loading for {table_name} skipped by user. Continuing to next table.")
                return False, False
            elif action == 'abort':
                logger.info(f"BigQuery loading for {table_name} aborted by user. Skipping all remaining tables.")
                return False, True
                
        except KeyboardInterrupt:
            logger.info("\nMigration aborted by user.")
            return False, True
        except Exception as e:
            logger.error(f"Error in BigQuery prompt: {e}")
            return False, True


def edit_schema(table_info: Dict[str, Any]) -> bool:
    """
    Edit custom schema for BigQuery table using an external editor.

    Args:
        table_info: Dictionary containing table information

    Returns:
        bool: True if schema was updated, False otherwise
    """
    table_name = format_table_name(table_info)
    current_schema = table_info.get('custom_schema', '')

    if current_schema:
        template_schema = current_schema
    else:
        inferred_schema = _generate_inferred_schema_json(table_info)
        if inferred_schema:
            template_schema = inferred_schema
            logger.info(f"\nUsing inferred schema from {table_name} columns and COPY query as starting point...")
        else:
            template_schema = '''[
  {
    "name": "id",
    "type": "INTEGER",
    "mode": "REQUIRED"
  },
  {
    "name": "name",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "created_at",
    "type": "TIMESTAMP",
    "mode": "NULLABLE"
  }
]'''
            logger.info(f"\nWARNING: Could not infer schema for {table_name}, using example template...")

    # Create a temporary file with the current schema or template
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as tmp_file:
        tmp_file.write(template_schema)
        tmp_file_path = tmp_file.name

    try:
        editor = os.environ.get('EDITOR')

        if not editor:
            if os.name == 'nt':  # Windows
                editor = 'notepad'
            else:  # Unix-like (Linux, macOS)
                for candidate in ['code', 'vim', 'nano', 'vi']:
                    if subprocess.run(['which', candidate], capture_output=True).returncode == 0:
                        editor = candidate
                        break
                else:
                    editor = 'vi'  # Last resort

        logger.info(f"\nOpening schema editor for {table_name} in {editor}...")
        logger.info("Edit the JSON schema as needed.")
        logger.info("Example field types: STRING, INTEGER, FLOAT, BOOLEAN, DATE, DATETIME, TIMESTAMP")
        logger.info("Example modes: REQUIRED, NULLABLE, REPEATED")
        logger.info("Save and exit when done, or delete all content to remove custom schema.")

        result = subprocess.run([editor, tmp_file_path])

        if result.returncode == 0:
            with open(tmp_file_path, 'r') as f:
                edited_schema = f.read().strip()

            if not edited_schema:
                if 'custom_schema' in table_info:
                    del table_info['custom_schema']
                    logger.info("Custom schema removed, will use auto-detection")
                return True

            try:
                import json
                parsed_schema = json.loads(edited_schema)

                if not isinstance(parsed_schema, list):
                    logger.info("WARNING: Schema must be an array of field objects")
                    return False

                for field in parsed_schema:
                    if not isinstance(field, dict) or 'name' not in field or 'type' not in field:
                        logger.info("WARNING: Each field must be an object with 'name' and 'type' properties")
                        return False

                table_info['custom_schema'] = edited_schema
                logger.info("Custom schema updated")

                logger.info(f"Parsed {len(parsed_schema)} field(s):")
                for field in parsed_schema:
                    mode = field.get('mode', 'NULLABLE')
                    logger.info(f"  - {field['name']}: {field['type']} ({mode})")

                return True

            except json.JSONDecodeError as e:
                logger.info(f"WARNING: Invalid JSON format: {e}")
                logger.info("Make sure to use proper JSON syntax with double quotes for strings")
                return False

        else:
            logger.info("Editor exited with error. Schema not updated.")
            return False

    except FileNotFoundError:
        logger.info(f"Editor '{editor}' not found. Available options:")
        logger.info("- Set EDITOR environment variable to your preferred editor")
        logger.info("- Install one of: code, vim, nano")
        logger.info("Schema not updated.")
        return False
    except Exception as e:
        logger.info(f"Error opening editor: {e}")
        logger.info("Schema not updated.")
        return False

    finally:
        try:
            os.unlink(tmp_file_path)
        except OSError:
            pass


def edit_partition_key(table_info: Dict[str, Any]) -> bool:
    """
    Edit partitioning settings for BigQuery table.
    
    Args:
        table_info: Dictionary containing table information
        
    Returns:
        bool: True if partitioning was updated, False otherwise
    """
    table_name = format_table_name(table_info)
    current_field = table_info.get('partition_field', '')
    current_type = table_info.get('partition_type', 'DAY')
    
    logger.info(f"\nEdit partitioning for {table_name}")
    if current_field:
        logger.info(f"Current: {current_field} ({current_type})")
    
    if not table_info.get('custom_schema'):
        inferred_schema = _generate_inferred_schema_json(table_info)
        if inferred_schema:
            table_info['custom_schema'] = inferred_schema
            logger.info("Auto-generated schema from table columns and COPY query for partitioning")
        else:
            logger.info("ERROR: Could not infer schema - partitioning requires schema definition")
            return False
    
    # Get partition-eligible columns (DATE, DATETIME, TIMESTAMP)
    eligible_columns = _get_partition_eligible_columns(table_info)
    
    if not eligible_columns:
        logger.info("ERROR: No columns suitable for partitioning found (need DATE, DATETIME, or TIMESTAMP columns)")
        logger.info("   Partitioning requires columns with temporal data types.")
        return False
    
    choices = ['(Remove partitioning)'] + [col['display'] for col in eligible_columns]
    
    current_choice = '(Remove partitioning)'
    for col in eligible_columns:
        if col['name'] == current_field:
            current_choice = col['display']
            break
    
    try:
        questions = [
            inquirer.List('partition_choice',
                         message=f"Select partition column for {table_name} (only DATE/DATETIME/TIMESTAMP columns shown)",
                         choices=choices,
                         default=current_choice)
        ]
        
        answers = inquirer.prompt(questions)
        if not answers:  # User pressed Ctrl+C
            return False
            
        partition_choice = answers['partition_choice']
    except KeyboardInterrupt:
        logger.info("\nPartitioning configuration cancelled.")
        return False
    
    if partition_choice == '(Remove partitioning)':
        for key in ['partition_field', 'partition_type']:
            if key in table_info:
                del table_info[key]
        logger.info("Partitioning removed")
        return True
    
    selected_column = None
    for col in eligible_columns:
        if col['display'] == partition_choice:
            selected_column = col
            break
    
    if not selected_column:
        logger.info("WARNING: Invalid column selection")
        return False
    
    try:
        questions = [
            inquirer.List('partition_type',
                         message=f"Partition type for {selected_column['name']} ({selected_column['type']})",
                         choices=[('DAY', 'DAY'), ('HOUR', 'HOUR'), ('MONTH', 'MONTH'), ('YEAR', 'YEAR')],
                         default=current_type)
        ]
        
        answers = inquirer.prompt(questions)
        if not answers:  # User pressed Ctrl+C
            return False
            
        partition_type = answers['partition_type']
    except KeyboardInterrupt:
        logger.info("\nPartitioning configuration cancelled.")
        return False
    
    table_info['partition_field'] = selected_column['name']
    table_info['partition_type'] = partition_type
    logger.info(f"Will partition by {selected_column['name']} ({partition_type})")
    return True


def edit_cluster_keys(table_info: Dict[str, Any]) -> bool:
    """
    Edit clustering settings for BigQuery table.
    
    Args:
        table_info: Dictionary containing table information
        
    Returns:
        bool: True if clustering was updated, False otherwise
    """
    table_name = format_table_name(table_info)
    current_fields = table_info.get('cluster_fields', [])
    
    logger.info(f"\nEdit clustering for {table_name}")
    if current_fields:
        logger.info(f"Current: {', '.join(current_fields)}")
    
    if not table_info.get('custom_schema'):
        inferred_schema = _generate_inferred_schema_json(table_info)
        if inferred_schema:
            table_info['custom_schema'] = inferred_schema
            logger.info("Auto-generated schema from table columns and COPY query for clustering")
        else:
            logger.info("ERROR: Could not infer schema - clustering requires schema definition")
            return False
    
    available_columns = _get_available_columns(table_info)
    
    if not available_columns:
        logger.info("ERROR: No columns found for clustering")
        return False
    
    logger.info("NOTE: BigQuery supports up to 4 clustering fields. Order matters for performance.")
    
    selected_columns = []
    
    # Allow user to select up to 4 columns
    for i in range(4):
        if i == 0:
            message = f"Select 1st clustering column for {table_name} (or select 'Done' to finish)"
        elif i == 1:
            message = f"Select 2nd clustering column (or select 'Done' to finish)"
        elif i == 2:
            message = f"Select 3rd clustering column (or select 'Done' to finish)"
        else:
            message = f"Select 4th clustering column (or select 'Done' to finish)"
        
        # Build choices (exclude already selected columns)
        already_selected = [col['name'] for col in selected_columns]
        choices = ['(Done - finish selection)', '(Remove all clustering)'] if selected_columns else ['(Remove all clustering)']
        
        for col in available_columns:
            if col['name'] not in already_selected:
                choices.append(col['display'])
        
        if len(choices) <= 2:  # Only "Done" and "Remove" options left
            break
        
        try:
            questions = [
                inquirer.List('column_choice',
                             message=message,
                             choices=choices,
                             default='(Done - finish selection)' if selected_columns else choices[1] if len(choices) > 1 else choices[0])
            ]
            
            answers = inquirer.prompt(questions)
            if not answers:  # User pressed Ctrl+C
                return False
                
            choice = answers['column_choice']
        except KeyboardInterrupt:
            logger.info("\nClustering configuration cancelled.")
            return False
        
        if choice == '(Done - finish selection)':
            break
        elif choice == '(Remove all clustering)':
            if 'cluster_fields' in table_info:
                del table_info['cluster_fields']
            logger.info("Clustering removed")
            return True
        else:
            for col in available_columns:
                if col['display'] == choice:
                    selected_columns.append(col)
                    break
    
    if not selected_columns:
        logger.info("ERROR: No columns selected for clustering")
        return False
    
    cluster_fields = [col['name'] for col in selected_columns]
    table_info['cluster_fields'] = cluster_fields
    
    column_names = [col['name'] for col in selected_columns]
    logger.info(f"Will cluster by: {', '.join(column_names)} (in this order)")
    return True
