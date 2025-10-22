"""
Essential tests for SnowflakeClient - just the main functionality.
"""
import pytest
from unittest.mock import Mock, patch, mock_open
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from snowflake.snowflake_client import SnowflakeClient, SnowflakeConnectionError


def test_client_initialization():
    """Test that client initializes properly."""
    client = SnowflakeClient("my_connection")
    assert client.connection_name == "my_connection"
    assert client.conn is None


@patch('snowflake.snowflake_client.snowflake.connector.connect')
def test_connection_context_manager(mock_connect):
    """Test the main connection workflow."""
    # Setup mock
    mock_conn = Mock()
    mock_cursor = Mock()
    mock_conn.cursor.return_value = mock_cursor
    mock_connect.return_value = mock_conn
    
    # Test context manager
    client = SnowflakeClient("test")
    with client as sf:
        assert sf.conn == mock_conn
        assert sf.cursor == mock_cursor
    
    # Verify connection was closed
    mock_conn.close.assert_called_once()


@patch('snowflake.snowflake_client.snowflake.connector.connect')
def test_connection_failure(mock_connect):
    """Test that connection failures are handled properly."""
    from snowflake.connector.errors import OperationalError
    mock_connect.side_effect = OperationalError("Failed")
    
    client = SnowflakeClient("test")
    with pytest.raises(SnowflakeConnectionError):
        with client:
            pass


def test_yaml_config_loading():
    """Test loading YAML configuration for tables."""
    yaml_content = """
    - database: TEST_DB
      schema: PUBLIC
      exclude_table_like:
        - 'TEMP_%'
    - database: PROD_DB
      table: important_table
    """
    
    client = SnowflakeClient()
    with patch('builtins.open', mock_open(read_data=yaml_content)):
        config = client._load_yaml_config("test.yml")
    
    assert len(config) == 2
    assert config[0]['database'] == 'TEST_DB'
    assert config[1]['table'] == 'important_table'


def test_copy_query_generation():
    """Test generating beautifully formatted COPY INTO queries."""
    client = SnowflakeClient()
    
    columns = [
        {'column_name': 'ID', 'data_type': 'NUMBER'},
        {'column_name': 'NAME', 'data_type': 'VARCHAR'},
        {'column_name': 'CREATED_AT', 'data_type': 'TIMESTAMP_TZ'},
        {'column_name': 'user.profile.name', 'data_type': 'VARCHAR'}  # Test dot replacement with underscore
    ]
    
    query = client.generate_copy_query(
        external_stage="my_stage",
        db="TEST_DB", 
        schema="PUBLIC",
        table="my_table",
        columns=columns,
        sample=False
    )
    
    # Check the essential components are there (now lowercase)
    assert 'COPY INTO @my_stage/test_db/public/my_table/' in query
    assert '"ID" AS "id"' in query
    assert '"CREATED_AT"::TIMESTAMP_NTZ' in query  # Timestamp casting
    assert '"user.profile.name" AS "user_profile_name"' in query  # Dot replacement with underscore
    assert 'PARQUET' in query
    assert 'OVERWRITE = TRUE' in query
    
    # Check the new beautiful formatting structure
    assert 'FROM (' in query  # Nested structure with parentheses
    assert 'SELECT\n    ' in query  # Proper SELECT formatting with newline and indentation
    assert ',\n    ' in query  # Columns separated by newlines with indentation
    assert 'FROM TEST_DB.PUBLIC.my_table' in query  # FROM clause on separate line
    assert ')' in query.split('FILE_FORMAT')[0]  # Closing parenthesis before file format
    
    # Test with sampling to check LIMIT formatting
    query_with_sample = client.generate_copy_query(
        external_stage="my_stage",
        db="TEST_DB", 
        schema="PUBLIC",
        table="my_table",
        columns=columns,
        sample=True
    )
    assert 'LIMIT ' in query_with_sample  # LIMIT clause should be present for sampling


def test_copy_query_execution():
    """Test executing COPY queries."""
    client = SnowflakeClient()
    client.conn = Mock()
    client.cursor = Mock()
    client.current_db = "TEST_DB"
    
    table_info = {
        'database': 'TEST_DB',
        'schema': 'PUBLIC', 
        'table': 'my_table',
        'cleaning_query': 'REMOVE @my_stage/test_db/public/my_table/',
        'copy_query': 'COPY INTO @stage/path FROM (SELECT * FROM table)'
    }
    
    success, error = client.run_copy_query(table_info, "my_stage")
    
    assert success is True
    assert error is None
    # Should call only COPY (cleaning is done separately now)
    assert client.cursor.execute.call_count >= 1


def test_end_to_end_workflow():
    """Test the main workflow: YAML -> tables -> queries."""
    yaml_content = """
    - database: TEST_DB
      schema: PUBLIC
    """
    
    # Mock query results (what Snowflake would return)
    mock_query_results = [
        {
            'DATABASE_NAME': 'TEST_DB',
            'SCHEMA_NAME': 'PUBLIC', 
            'TABLE_NAME': 'MY_TABLE',
            'COLUMN_NAME': 'ID',
            'DATA_TYPE': 'NUMBER',
            'TABLE_TYPE': 'BASE TABLE'
        },
        {
            'DATABASE_NAME': 'TEST_DB',
            'SCHEMA_NAME': 'PUBLIC',
            'TABLE_NAME': 'MY_TABLE', 
            'COLUMN_NAME': 'NAME',
            'DATA_TYPE': 'VARCHAR',
            'TABLE_TYPE': 'BASE TABLE'
        }
    ]
    
    client = SnowflakeClient()
    client.conn = Mock()
    client.cursor = Mock()
    
    with patch('builtins.open', mock_open(read_data=yaml_content)):
        with patch.object(client, 'execute_query', return_value=mock_query_results):
            tables = client.list_tables_from_yaml("test.yml")
    
    # Should find one table with two columns
    assert len(tables) == 1
    assert tables[0]['database'] == 'TEST_DB'
    assert tables[0]['table'] == 'MY_TABLE'
    assert len(tables[0]['columns']) == 2
    
    # Generate queries for the tables
    tables_with_queries = client.generate_copy_queries(tables, "my_stage")
    
    # Should have copy query
    assert 'copy_query' in tables_with_queries[0]
    assert 'COPY INTO' in tables_with_queries[0]['copy_query']


def test_bigquery_column_normalization():
    """Test BigQuery V2 column name normalization."""
    client = SnowflakeClient("test_connection")
    
    # Test various column name normalization cases (now lowercase)
    test_cases = [
        # (input, expected_output)
        ('CUSTOMER_ID', 'customer_id'),                      # already valid -> lowercase
        ('customer.name', 'customer_name'),                  # dot replacement
        ('user profile email', 'user_profile_email'),       # spaces
        ('order-date', 'order_date'),                        # hyphen
        ('2nd_priority', '_2nd_priority'),                   # starts with number
        ('user@domain.com', 'user_domain_com'),              # special characters
        ('"quoted.column"', 'quoted_column'),                # quoted with dot
        ('trailing_underscores___', 'trailing_underscores'), # trailing underscores
        ('', '_'),                                            # empty string -> _ (not _column)
        ('_TABLE_reserved', '_table_reserved_'),              # reserved prefix -> lowercase
    ]
    
    for input_name, expected in test_cases:
        result = client._normalize_column_name_for_bigquery(input_name)
        assert result == expected, f"Expected {input_name} -> {expected}, got {result}"


def test_column_expression_with_normalization():
    """Test column expression building with BigQuery V2 normalization."""
    client = SnowflakeClient("test_connection")
    
    # Test regular column (now lowercase)
    column = {'column_name': 'customer.id', 'data_type': 'INTEGER'}
    expression = client._build_column_expression(column, cast_timestamp_to_string=False)
    expected = '"customer.id" AS "customer_id"'
    assert expression == expected, f"Expected: {expected}, got: {expression}"
    
    # Test timestamp column with casting (now lowercase)
    timestamp_column = {'column_name': 'order.date', 'data_type': 'TIMESTAMP_TZ'}
    expression = client._build_column_expression(timestamp_column, cast_timestamp_to_string=True)
    expected = '"order.date"::TIMESTAMP_NTZ AS "order_date"'
    assert expression == expected, f"Expected: {expected}, got: {expression}"
    
    # Test column with special characters (now lowercase)
    special_column = {'column_name': 'user email@domain', 'data_type': 'VARCHAR'}
    expression = client._build_column_expression(special_column, cast_timestamp_to_string=False)
    expected = '"user email@domain" AS "user_email_domain"'
    assert expression == expected, f"Expected: {expected}, got: {expression}"


@patch('snowflake.snowflake_client.snowflake.connector.connect')
def test_get_table_row_count(mock_connect):
    """Test getting row count from Snowflake table."""
    mock_conn = Mock()
    mock_cursor = Mock()
    mock_cursor.fetchone.return_value = [5000]
    mock_conn.cursor.return_value = mock_cursor
    mock_connect.return_value = mock_conn
    
    client = SnowflakeClient("test")
    
    with client:
        count = client.get_table_row_count("PROD", "ANALYTICS", "SALES")
    
    assert count == 5000
    mock_cursor.execute.assert_called_once()
    call_args = mock_cursor.execute.call_args[0][0]
    assert "SELECT COUNT(*) as count FROM PROD.ANALYTICS.SALES" in call_args


if __name__ == "__main__":
    logger.info("Running essential SnowflakeClient tests...")
    pytest.main([__file__, "-v"])
