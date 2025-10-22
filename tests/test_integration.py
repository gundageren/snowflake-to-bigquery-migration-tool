"""
Essential integration tests - just the key workflows.
Since we already have solid unit tests, keeping this minimal.

Note: All test artifacts are isolated in tests/logs/ directory.
"""
import pytest
import os
import sys
import shutil
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from main import main, parse_arguments


class TestEssentialIntegration:
    """Just the essential end-to-end tests."""
    
    def setup_method(self):
        """Set up test environment with isolated logs directory."""
        self.test_logs_dir = Path('tests/logs')
        self.test_logs_dir.mkdir(parents=True, exist_ok=True)
    
    def teardown_method(self):
        """Clean up test artifacts after each test."""
        # Note: We don't clean up immediately because atexit handlers might still run
        # Let pytest handle cleanup or clean manually if needed

    def test_argument_parsing(self):
        """Test CLI arguments work."""
        with patch('sys.argv', ['main.py', '--dry_run', '--sample']):
            args = parse_arguments()
            assert args.dry_run is True
            assert args.sample is True

    @patch('main.SnowflakeClient')
    @patch('main.BigQueryClient')
    def test_dry_run_workflow(self, mock_bq_class, mock_sf_class):
        """Test complete dry run - the most common use case."""
        # Setup mocks
        mock_sf_context = MagicMock()
        mock_bq_context = MagicMock()
        mock_sf_class.return_value.__enter__.return_value = mock_sf_context
        mock_bq_class.return_value.__enter__.return_value = mock_bq_context
        
        mock_sf_context.list_tables_from_yaml.return_value = [
            {'database': 'TEST_DB', 'schema': 'PUBLIC', 'table': 'test_table'}
        ]
        mock_sf_context.generate_copy_queries.return_value = [
            {
                'database': 'TEST_DB',
                'schema': 'PUBLIC', 
                'table': 'test_table',
                'copy_query': 'COPY INTO @stage FROM table;'
            }
        ]
        
        env_vars = {
            'EXTERNAL_STAGE': 'test.stage',
            'PROJECT_ID': 'test-project',
            'GCS_URI': 'gs://test-bucket',
            'LOGS_PATH': 'tests/logs'
        }
        
        with patch.dict(os.environ, env_vars):
            main(dry_run=True, interactive=False, sample=False)
            
            # Verify dry run behavior - no actual execution
            mock_sf_context.run_copy_query.assert_not_called()
            mock_bq_context.create_bq_table.assert_not_called()

    @patch('main.SnowflakeClient')
    @patch('main.BigQueryClient')
    def test_actual_migration_workflow(self, mock_bq_class, mock_sf_class):
        """Test actual migration (not dry run)."""
        mock_sf_context = MagicMock()
        mock_bq_context = MagicMock()
        mock_sf_class.return_value.__enter__.return_value = mock_sf_context
        mock_bq_class.return_value.__enter__.return_value = mock_bq_context
        
        mock_sf_context.list_tables_from_yaml.return_value = [
            {'database': 'TEST_DB', 'schema': 'PUBLIC', 'table': 'test_table'}
        ]
        mock_sf_context.generate_copy_queries.return_value = [
            {
                'database': 'TEST_DB',
                'schema': 'PUBLIC',
                'table': 'test_table',
                'copy_query': 'COPY INTO @stage FROM table;'
            }
        ]
        mock_sf_context.run_cleaning_query.return_value = (True, None)
        mock_sf_context.run_copy_query.return_value = (True, None)
        mock_sf_context.get_table_row_count.return_value = 100
        mock_bq_context.create_bq_table.return_value = (True, None, 100)
        
        env_vars = {
            'EXTERNAL_STAGE': 'test.stage',
            'PROJECT_ID': 'test-project',
            'GCS_URI': 'gs://test-bucket',
            'LOGS_PATH': 'tests/logs'
        }
        
        with patch.dict(os.environ, env_vars):
            main(dry_run=False, interactive=False, sample=False)
            
            # Verify actual migration happened
            mock_sf_context.run_copy_query.assert_called_once()
            mock_bq_context.create_bq_table.assert_called_once()

    @patch('inquirer.prompt')
    @patch('main.SnowflakeClient')
    @patch('main.BigQueryClient')
    @patch('config.load_dotenv')
    def test_interactive_bigquery_permission(self, mock_load_dotenv, mock_bq_class, mock_sf_class, mock_prompt):
        """Test that interactive mode prompts for BigQuery table loading permission."""
        # Mock user responses - first for table permission, then for BigQuery loading
        mock_prompt.side_effect = [
            {'action': 'proceed'},  # Proceed with table migration
            {'action': 'proceed'}   # Proceed with BigQuery loading
        ]
        
        # Mock clients
        mock_sf_context = Mock()
        mock_bq_context = Mock()
        mock_sf_class.return_value.__enter__.return_value = mock_sf_context
        mock_bq_class.return_value.__enter__.return_value = mock_bq_context
        
        # Mock successful operations
        mock_sf_context.list_tables_from_yaml.return_value = [
            {'database': 'TEST_DB', 'schema': 'PUBLIC', 'table': 'test_table'}
        ]
        mock_sf_context.generate_copy_queries.return_value = [
            {
                'database': 'TEST_DB', 'schema': 'PUBLIC', 'table': 'test_table',
                'copy_query': 'COPY INTO @stage FROM table;',
                'cleaning_query': 'DELETE FROM @stage WHERE 1=1;'
            }
        ]
        mock_sf_context.run_cleaning_query.return_value = (True, None)
        mock_sf_context.run_copy_query.return_value = (True, None)
        mock_sf_context.get_table_row_count.return_value = 100
        mock_bq_context.create_bq_table.return_value = (True, None, 100)
        
        env_vars = {
            'EXTERNAL_STAGE': 'test.stage',
            'PROJECT_ID': 'test-project',
            'GCS_URI': 'gs://test-bucket',
            'LOGS_PATH': 'tests/logs'
        }
        
        with patch.dict(os.environ, env_vars):
            main(dry_run=False, interactive=True, sample=False)
            
            # Verify BigQuery table was called
            mock_bq_context.create_bq_table.assert_called_once()
            
            # Verify we got two input prompts (table permission + BigQuery permission)
            assert mock_prompt.call_count == 2

    @patch('subprocess.run')
    @patch('builtins.open')
    @patch('tempfile.NamedTemporaryFile')
    @patch('inquirer.prompt')
    @patch('main.SnowflakeClient')
    @patch('main.BigQueryClient')
    @patch('config.load_dotenv')
    def test_bigquery_edit_options(self, mock_load_dotenv, mock_bq_class, mock_sf_class, mock_prompt, mock_temp_file, mock_open, mock_subprocess):
        """Test BigQuery edit options in interactive mode."""
        # Mock user responses - edit schema via external editor, then proceed
        mock_prompt.side_effect = [
            {'action': 'proceed'},     # Proceed with table migration
            {'action': 'edit-schema'}, # Edit BigQuery schema
            {'action': 'proceed'}      # Proceed with BigQuery loading after schema edit
        ]
        
        # Mock the temporary file and editor process
        mock_temp_file.return_value.__enter__.return_value.name = '/tmp/test_schema.json'
        mock_subprocess.return_value.returncode = 0
        
        # Mock reading the edited schema file
        mock_file_handle = mock_open.return_value.__enter__.return_value
        mock_file_handle.read.return_value = '[{"name": "id", "type": "INTEGER"}]'
        
        # Mock clients
        mock_sf_context = Mock()
        mock_bq_context = Mock()
        mock_sf_class.return_value.__enter__.return_value = mock_sf_context
        mock_bq_class.return_value.__enter__.return_value = mock_bq_context
        
        # Mock successful operations
        mock_sf_context.list_tables_from_yaml.return_value = [
            {'database': 'TEST_DB', 'schema': 'PUBLIC', 'table': 'test_table'}
        ]
        mock_sf_context.generate_copy_queries.return_value = [
            {
                'database': 'TEST_DB', 'schema': 'PUBLIC', 'table': 'test_table',
                'copy_query': 'COPY INTO @stage FROM table;',
                'cleaning_query': 'DELETE FROM @stage WHERE 1=1;'
            }
        ]
        mock_sf_context.run_cleaning_query.return_value = (True, None)
        mock_sf_context.run_copy_query.return_value = (True, None)
        mock_sf_context.get_table_row_count.return_value = 100
        mock_bq_context.create_bq_table.return_value = (True, None, 100)
        
        env_vars = {
            'EXTERNAL_STAGE': 'test.stage',
            'PROJECT_ID': 'test-project',
            'GCS_URI': 'gs://test-bucket',
            'LOGS_PATH': 'tests/logs'
        }
        
        with patch.dict(os.environ, env_vars):
            main(dry_run=False, interactive=True, sample=False)
            
            # Verify BigQuery table was called with custom schema
            mock_bq_context.create_bq_table.assert_called_once()
            call_args = mock_bq_context.create_bq_table.call_args[0][0]
            assert 'custom_schema' in call_args

    @patch('main.SnowflakeClient')
    @patch('main.BigQueryClient')
    @patch('config.load_dotenv')  # Prevent .env file loading
    def test_config_validation_error(self, mock_load_dotenv, mock_bq_class, mock_sf_class):
        """Test config validation catches missing env vars."""
        with patch.dict(os.environ, {'LOGS_PATH': 'tests/logs'}, clear=True):
            with pytest.raises(ValueError, match="EXTERNAL_STAGE"):
                main(dry_run=False, interactive=False, sample=False)


if __name__ == "__main__":
    logger.info("Running essential integration tests...")
    pytest.main([__file__, "-v"])
