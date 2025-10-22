"""
Essential tests for BigQueryClient - just the main functionality.
"""
import pytest
from unittest.mock import Mock, patch
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from bigquery.bigquery_client import (
    BigQueryClient, 
    BigQueryConnectionError, 
    BigQueryDatasetError
)


def test_client_initialization():
    """Test that client initializes properly."""
    client = BigQueryClient(
        project_id="test-project",
        gcs_uri="gs://test-bucket",
        location="EU"
    )
    assert client.project_id == "test-project"
    assert client.gcs_uri == "gs://test-bucket"
    assert client.location == "EU"
    assert client.client is None


def test_initialization_with_defaults():
    """Test client with default values."""
    client = BigQueryClient(
        project_id="test-project",
        gcs_uri="gs://test-bucket/"  # With trailing slash
    )
    assert client.location == "EU"  # Default
    assert client.dataset_prefix == "snowflake_"  # Default
    assert client.gcs_uri == "gs://test-bucket"  # Trailing slash removed


@patch('bigquery.bigquery_client.bigquery.Client')
def test_connection_context_manager(mock_bigquery_client):
    """Test the main connection workflow."""
    # Setup mock
    mock_bq_client = Mock()
    mock_bigquery_client.return_value = mock_bq_client
    
    # Test context manager
    client = BigQueryClient("test-project", "gs://test-bucket")
    with client as bq:
        assert bq.client == mock_bq_client
    
    # Verify client was closed
    mock_bq_client.close.assert_called_once()


@patch('bigquery.bigquery_client.bigquery.Client')
def test_connection_failure(mock_bigquery_client):
    """Test that connection failures are handled properly."""
    from google.cloud.exceptions import GoogleCloudError
    mock_bigquery_client.side_effect = GoogleCloudError("Failed")
    
    client = BigQueryClient("test-project", "gs://test-bucket")
    with pytest.raises(BigQueryConnectionError):
        with client:
            pass


def test_helper_methods():
    """Test utility helper methods."""
    client = BigQueryClient("test-project", "gs://test-bucket")
    
    # Test dataset ID building
    dataset_id = client._build_dataset_id("TEST_DB", "PUBLIC")
    assert dataset_id == "snowflake_test_db_public"
    
    # Test GCS URI building
    gcs_uri = client._build_gcs_source_uri("TEST_DB", "PUBLIC", "MY_TABLE")
    assert gcs_uri == "gs://test-bucket/test_db/public/my_table/*"


def test_load_job_config():
    """Test BigQuery load job configuration."""
    client = BigQueryClient("test-project", "gs://test-bucket")
    
    job_config = client._create_load_job_config()
    
    # Check the main settings
    from google.cloud import bigquery
    assert job_config.source_format == bigquery.SourceFormat.PARQUET
    assert job_config.write_disposition == bigquery.WriteDisposition.WRITE_TRUNCATE
    assert job_config.autodetect is True
    assert job_config.column_name_character_map == "V2"


def test_external_table_config():
    """Test external table configuration."""
    client = BigQueryClient("test-project", "gs://test-bucket")
    
    config = client._create_external_config("gs://test-bucket/path/to/data/*")
    
    assert config.source_format == "PARQUET"
    assert config.source_uris == ["gs://test-bucket/path/to/data/*"]
    assert config.autodetect is True


@patch('bigquery.bigquery_client.bigquery.Client')
def test_create_table_success(mock_bigquery_client):
    """Test successful table creation."""
    # Setup mocks
    mock_bq_client = Mock()
    mock_bigquery_client.return_value = mock_bq_client
    
    # Mock dataset and table operations
    mock_dataset_ref = Mock()
    mock_table_ref = Mock()
    mock_bq_client.dataset.return_value = mock_dataset_ref
    mock_dataset_ref.table.return_value = mock_table_ref
    
    # Mock load job
    mock_load_job = Mock()
    mock_bq_client.load_table_from_uri.return_value = mock_load_job
    
    # Mock final table with row count
    mock_final_table = Mock()
    mock_final_table.num_rows = 1000
    mock_bq_client.get_table.return_value = mock_final_table
    
    client = BigQueryClient("test-project", "gs://test-bucket")
    client.client = mock_bq_client
    
    table_info = {
        'database': 'TEST_DB',
        'schema': 'PUBLIC',
        'table': 'my_table'
    }
    
    success, error, row_count = client.create_bq_table(table_info)
    
    assert success is True
    assert error is None
    assert row_count == 1000
    
    # Verify the load job was called
    mock_bq_client.load_table_from_uri.assert_called_once()
    mock_load_job.result.assert_called_once()


@patch('bigquery.bigquery_client.bigquery.Client')
def test_create_external_table_success(mock_bigquery_client):
    """Test successful external table creation."""
    # Setup mocks
    mock_bq_client = Mock()
    mock_bigquery_client.return_value = mock_bq_client
    
    # Mock dataset and table operations
    mock_dataset_ref = Mock()
    mock_table_ref = Mock()
    mock_bq_client.dataset.return_value = mock_dataset_ref
    mock_dataset_ref.table.return_value = mock_table_ref
    
    # Mock table deletion (table doesn't exist)
    from google.cloud.exceptions import NotFound
    mock_bq_client.get_table.side_effect = NotFound("Table not found")
    
    client = BigQueryClient("test-project", "gs://test-bucket")
    client.client = mock_bq_client
    
    table_info = {
        'database': 'TEST_DB',
        'schema': 'PUBLIC',
        'table': 'my_table'
    }
    
    success, error = client.create_bq_external_table(table_info)
    
    assert success is True
    assert error is None
    
    # Verify external table was created
    mock_bq_client.create_table.assert_called_once()


def test_ensure_client_no_connection():
    """Test client check when no connection."""
    client = BigQueryClient("test-project", "gs://test-bucket")
    # No client set
    
    with pytest.raises(BigQueryConnectionError, match="No active BigQuery client"):
        client._ensure_client()


def test_ensure_client_with_connection():
    """Test client check when connection exists."""
    client = BigQueryClient("test-project", "gs://test-bucket")
    client.client = Mock()  # Set a mock client
    
    # Should not raise an exception
    client._ensure_client()


@patch('bigquery.bigquery_client.bigquery.Client')
def test_end_to_end_table_loading(mock_bigquery_client):
    """Test the complete table loading workflow."""
    # Setup comprehensive mocks
    mock_bq_client = Mock()
    mock_bigquery_client.return_value = mock_bq_client
    
    # Mock all the BigQuery objects
    mock_dataset_ref = Mock()
    mock_table_ref = Mock()
    mock_load_job = Mock()
    mock_final_table = Mock()
    
    mock_bq_client.dataset.return_value = mock_dataset_ref
    mock_dataset_ref.table.return_value = mock_table_ref
    mock_bq_client.load_table_from_uri.return_value = mock_load_job
    mock_final_table.num_rows = 5000
    mock_bq_client.get_table.return_value = mock_final_table
    
    # Test with context manager and table loading
    client = BigQueryClient(
        project_id="my-project",
        gcs_uri="gs://my-bucket",
        dataset_prefix="test_"
    )
    
    with client as bq:
        table_info = {
            'database': 'ANALYTICS_DB',
            'schema': 'REPORTING',
            'table': 'SALES_DATA'
        }
        
        success, error, row_count = bq.create_bq_table(table_info)
    
        assert success is True
        assert error is None
        assert row_count == 5000
        
        # Verify the dataset was created with correct naming
        mock_bq_client.create_dataset.assert_called()
        
        # Verify load job was configured and executed
        mock_bq_client.load_table_from_uri.assert_called_once()
        mock_load_job.result.assert_called_once()


def test_snowflake_type_conversion():
    """Test Snowflake to BigQuery type conversion."""
    client = BigQueryClient("test-project", "gs://test-bucket")
    
    test_cases = [
        ('VARCHAR(255)', 'STRING'),
        ('NUMBER(10,2)', 'NUMERIC'), 
        ('INTEGER', 'INTEGER'),
        ('TIMESTAMP_NTZ', 'TIMESTAMP'),
        ('BOOLEAN', 'BOOLEAN'),
        ('DATE', 'DATE'),
        ('VARIANT', 'JSON'),
        ('UNKNOWN_TYPE', 'STRING'),  # fallback
    ]
    
    for sf_type, expected_bq_type in test_cases:
        result = client._convert_snowflake_type_to_bigquery(sf_type)
        assert result == expected_bq_type, f"Expected {sf_type} -> {expected_bq_type}, got {result}"


def test_column_name_normalization_v2():
    """Test BigQuery V2 column name normalization with lowercase."""
    client = BigQueryClient("test-project", "gs://test-bucket")
    
    test_cases = [
        ('CUSTOMER_ID', 'customer_id'),                      # uppercase -> lowercase
        ('customer.name', 'customer_name'),                  # dot replacement
        ('user profile email', 'user_profile_email'),       # spaces
        ('order-date', 'order_date'),                        # hyphen
        ('2nd_priority', '_2nd_priority'),                   # starts with number
        ('user@domain.com', 'user_domain_com'),              # special characters
        ('trailing_underscores___', 'trailing_underscores'), # trailing underscores
        ('', '_'),                                            # empty string -> _
        ('_TABLE_reserved', '_table_reserved_'),              # reserved prefix -> lowercase
    ]
    
    for input_name, expected in test_cases:
        result = client._normalize_column_name_v2(input_name)
        assert result == expected, f"Expected {input_name} -> {expected}, got {result}"


def test_schema_inference_with_deduplication():
    """Test schema inference with duplicate column name handling."""
    client = BigQueryClient("test-project", "gs://test-bucket")
    
    # Table info with columns that will create duplicates after normalization
    table_info = {
        'columns': [
            {'column_name': '', 'data_type': 'INTEGER'},              # -> _
            {'column_name': '   ', 'data_type': 'VARCHAR(50)'},       # -> _ (duplicate!)
            {'column_name': 'user.name', 'data_type': 'VARCHAR(255)'}, # -> user_name
            {'column_name': 'user-name', 'data_type': 'VARCHAR(255)'}, # -> user_name (duplicate!)
            {'column_name': 'USER_NAME', 'data_type': 'VARCHAR(255)'}, # -> user_name (duplicate!)
        ],
        'copy_query': '''COPY INTO @stage/ FROM (
            SELECT
                \"\" AS \"\",
                \"   \" AS \"   \",
                \"user.name\" AS \"user_name\",
                \"user-name\" AS \"user_name\", 
                \"USER_NAME\" AS \"USER_NAME\"
            FROM test_table
        )'''
    }
    
    schema_fields = client.infer_schema_from_table_info(table_info)
    
    # Check that we have all fields
    assert len(schema_fields) == 5
    
    # Check that all names are unique
    field_names = [field.name for field in schema_fields]
    assert len(field_names) == len(set(field_names)), "Duplicate field names found"
    
    # Check that names are lowercase and properly deduplicated
    # Logical deduplication: _ -> _2, _3; user_name -> user_name_2, user_name_3 (starting at 2)
    expected_names = {'_', '_2', 'user_name', 'user_name_2', 'user_name_3'}
    actual_names = set(field_names)
    assert actual_names == expected_names, f"Expected {expected_names}, got {actual_names}"


def test_column_alias_extraction():
    """Test extraction of column aliases from COPY queries."""
    client = BigQueryClient("test-project", "gs://test-bucket")
    
    copy_query = '''COPY INTO @stage/ FROM (
        SELECT
            \"customer.id\" AS \"customer_id\",
            \"order date\" AS \"order_date\", 
            \"user@email\" AS \"user_email\"
        FROM table
    )'''
    
    aliases = client._extract_column_aliases_from_copy_query(copy_query)
    expected_aliases = ['customer_id', 'order_date', 'user_email']
    
    assert aliases == expected_aliases, f"Expected {expected_aliases}, got {aliases}"


@patch('bigquery.bigquery_client.bigquery.Client')
def test_get_table_row_count(mock_bigquery_client):
    """Test getting row count from BigQuery table."""
    mock_bq_client = Mock()
    mock_bigquery_client.return_value = mock_bq_client
    
    mock_result = Mock()
    mock_result.__iter__ = Mock(return_value=iter([{'count': 5000}]))
    mock_bq_client.query.return_value.result.return_value = mock_result
    
    client = BigQueryClient("test-project", "gs://test-bucket")
    
    with client:
        count = client.get_table_row_count("test_dataset", "test_table")
    
    assert count == 5000


@patch('bigquery.bigquery_client.bigquery.Client')
def test_create_bq_table_with_autodetect_and_partitioning(mock_bigquery_client):
    """Test table creation with autodetect when partitioning is specified but no custom schema."""
    mock_bq_client = Mock()
    mock_bigquery_client.return_value = mock_bq_client
    
    mock_dataset = Mock()
    mock_bq_client.dataset.return_value = mock_dataset
    mock_bq_client.create_dataset.return_value = mock_dataset
    
    mock_table = Mock()
    mock_table.num_rows = 1000
    mock_bq_client.get_table.return_value = mock_table
    
    mock_load_job = Mock()
    mock_load_job.result.return_value = None
    mock_bq_client.load_table_from_uri.return_value = mock_load_job
    
    client = BigQueryClient("test-project", "gs://test-bucket")
    
    table_info = {
        'database': 'PROD',
        'schema': 'ANALYTICS',
        'table': 'SALES_DATA',
        'partition_field': 'event_date',
        'partition_type': 'DAY',
        'cluster_fields': ['customer_id', 'region']
    }
    
    with client:
        success, error, row_count = client.create_bq_table(table_info)
    
    assert success is True
    assert error is None
    assert row_count == 1000
    
    load_call = mock_bq_client.load_table_from_uri.call_args
    job_config = load_call.kwargs['job_config'] if 'job_config' in load_call.kwargs else load_call[0][2] if len(load_call[0]) > 2 else None
    
    assert job_config is not None
    assert job_config.autodetect is True
    assert job_config.time_partitioning is not None
    assert job_config.time_partitioning.field == 'event_date'
    assert job_config.clustering_fields == ['customer_id', 'region']


if __name__ == "__main__":
    logger.info("Running essential BigQueryClient tests...")
    pytest.main([__file__, "-v"])
