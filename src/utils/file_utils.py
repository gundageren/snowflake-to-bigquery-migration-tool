import logging
from collections import defaultdict
from typing import List, Dict, Any

import yaml
from tabulate import tabulate

MSG_NO_TABLES = "No tables were found to count."

logger = logging.getLogger(__name__)


class KeyAwareDumper(yaml.SafeDumper):
    """Custom YAML dumper that tracks current key context for smart formatting."""
    
    def __init__(self, stream, **kwargs):
        super().__init__(stream, **kwargs)
        self._current_key = None
    
    def represent_mapping(self, tag, mapping, flow_style=None):
        """Override to track current key being processed."""
        value = []
        node = yaml.MappingNode(tag, value, flow_style=flow_style)
        if self.alias_key is not None:
            self.represented_objects[self.alias_key] = node
        
        if hasattr(mapping, 'items'):
            mapping = mapping.items()
        for item_key, item_value in mapping:
            # Track the current key for context-aware string formatting
            self._current_key = item_key
            node_key = self.represent_data(item_key)
            node_value = self.represent_data(item_value)
            value.append((node_key, node_value))
            
        if flow_style is None:
            node.flow_style = self.default_flow_style
        else:
            node.flow_style = flow_style
            
        return node


def represent_str_simple(dumper, data) -> Any:
    """
    Simple key-aware YAML representer: quotes everything, literal blocks for specific multiline keys.
    
    Args:
        dumper: KeyAwareDumper instance with current key context
        data: String data to represent
        
    Returns:
        YAML scalar representation
    """
    current_key = getattr(dumper, '_current_key', None)
    
    # Use literal block scalars for specific multiline keys
    if current_key in ('cleaning_query', 'copy_query', 'custom_schema') and '\n' in data:
        return dumper.represent_scalar('tag:yaml.org,2002:str', data, style='|')
    
    # Quote everything else
    return dumper.represent_scalar('tag:yaml.org,2002:str', data, style="'")


# Register the simple key-aware representer
KeyAwareDumper.add_representer(str, represent_str_simple)


def log_table_counts(table_list: List[Dict[str, Any]]) -> None:
    """
    Log count of tables grouped by database and schema.

    Args:
        table_list: List of table dictionaries to count
    """
    if not table_list:
        logger.warning(MSG_NO_TABLES)
        return

    schema_counts = defaultdict(int)
    for table_info in table_list:
        key = f"{table_info['database']}.{table_info['schema']}"
        schema_counts[key] += 1

    table_data = [[schema, count] for schema, count in sorted(schema_counts.items())]
    table_data.append(["-" * 40, "-" * 20])
    table_data.append(["Total", sum(schema_counts.values())])

    formatted_table = tabulate(
        table_data, 
        headers=["Schema", "Number of Tables"], 
        tablefmt="github"
    )
    logger.info("\n" + formatted_table)


def write_results_on_exit(succeeded_tables: List[Dict], failed_tables: List[Dict], 
                         succeeded_file: str, failed_file: str) -> None:
    """
    Write final results to YAML files upon script exit.
    
    Args:
        succeeded_tables: List of successfully processed tables
        failed_tables: List of failed/skipped tables  
        succeeded_file: Path to success results file
        failed_file: Path to failure results file
    """
    with open(succeeded_file, 'w') as f:
        if succeeded_tables:
            yaml.dump(succeeded_tables, f, Dumper=KeyAwareDumper, default_flow_style=False, sort_keys=False, allow_unicode=True)
        else:
            f.write('')
    logger.info(f"Successfully processed {len(succeeded_tables)} table(s). Details in '{succeeded_file}'.")

    with open(failed_file, 'w') as f:
        if failed_tables:
            yaml.dump(failed_tables, f, Dumper=KeyAwareDumper, default_flow_style=False, sort_keys=False, allow_unicode=True)
        else:
            f.write('')
    
    if failed_tables:
        logger.error(f"Failed to process {len(failed_tables)} table(s). Details in '{failed_file}'.")
    else:
        logger.info("All tables processed successfully!")
