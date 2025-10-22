# Snowflake to BigQuery Data Migration Tool

A Python tool to migrate table data from Snowflake to BigQuery via Google Cloud Storage. Built for reliability and ease of use.

## Quick Start

1. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

2. **Set up your `.env` file** (see [Environment Variables](#environment-variables))

3. **Configure tables to migrate** in `config/tables.yml`

4. **Test your setup:**
   ```bash
   python src/main.py --dry_run
   ```

5. **Run the migration:**
   ```bash
   python src/main.py
   ```

## What It Does

**Simple end-to-end pipeline:**
- **Reads config** → Determines which Snowflake tables/views to migrate
- **Generates queries** → Creates optimized COPY INTO statements  
- **Exports data** → Unloads from Snowflake to GCS as Parquet files
- **Loads to BigQuery** → Creates tables and imports the data
- **Validates migration** → Compares row counts and ensures data integrity
- **Tracks everything** → Detailed logging and success/failure reports

**Key features:**
- **Data validation** - Automatic row count comparison between Snowflake and BigQuery with console + log output
- **Auto-detect partitioning** - Use schema autodetect when partition/cluster fields specified without custom schema
- **Smart schema inference** - Automatically infer BigQuery schemas from Snowflake column metadata and COPY query aliases
- **Visual column selection** - Interactive prompts for partition/cluster keys with type filtering and validation
- **Advanced BigQuery features** - Full support for partitioning, clustering, and custom schemas with auto-generation
- **Dry run mode** - Analyze tables and generate queries without executing, saves timestamped analysis files
- **Interactive mode** - Ask permission for each table with optional query preview
- **Sample mode** - Test with limited rows (default: 100) for quick validation
- **Verbose mode** - Show cleaning and copy queries during execution for transparency
- **Beautiful query formatting** - Generate clean, readable SQL with proper indentation
- **Query editing** - Edit cleaning and copy queries on failure using external editors
- **Robust error handling** - Continues on failures, comprehensive retry options
- **Comprehensive logging** - Detailed timestamped logs for analysis and debugging

## Prerequisites

- **Python 3.8+** installed
- **Snowflake account** with data to migrate
- **Google Cloud Platform** project with BigQuery enabled
- **GCS bucket** for temporary storage

## Installation

```bash
# Clone the repo
git clone git@github.com:gundageren/snowflake-to-bigquery-migration-tool.git
cd snowflake-to-bigquery-migration-tool

# Install dependencies
pip install -r requirements.txt
```

## Configuration

### 1. Create GCS Bucket
Set up a Google Cloud Storage (GCS) bucket that will serve as the destination for unloading data from Snowflake.

### 2. Create Snowflake Integration
Snowflake uses named integration objects to securely connect with cloud storage services. These objects eliminate the need to manually provide cloud credentials like secret keys or access tokens. Instead, they reference a designated Cloud Storage service account.

```sql
USE ROLE ACCOUNTADMIN;

CREATE STORAGE INTEGRATION gcs_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = GCS
  ENABLED = TRUE
  STORAGE_ALLOWED_LOCATIONS = ('gcs://bucket');
```

### 3. Obtain GCP Service Account
When you create a STORAGE INTEGRATION object for GCS in Snowflake, it automatically generates a corresponding GCP service account.

```sql
DESCRIBE INTEGRATION gcs_integration
```

Locate the STORAGE_GCP_SERVICE_ACCOUNT property in the output. This contains the name of the GCP service account.

### 4. Create GCP Service Account
In the Google Cloud Console, create a new service account using the value from the STORAGE_GCP_SERVICE_ACCOUNT property as the account name.

### 5. Grant Permission to Service Account
Modify the permissions of your GCS bucket to add the newly created service account as a member. Assign it the Storage Object Admin role to enable full access to objects within the bucket.

### 6. Create File Formats
To unload data in Parquet format, define a file format object in Snowflake using the appropriate Parquet settings.

```sql
USE DB_NAME;
USE SCHEMA_NAME;
CREATE OR REPLACE FILE FORMAT parquet_unload_file_format
TYPE = PARQUET
SNAPPY_COMPRESSION = TRUE
COMMENT = 'FILE FORMAT FOR UNLOADING AS PARQUET FILES';
```

### 7. Create External Stages
Use an EXTERNAL STAGE object to point to the GCS bucket. This object allows Snowflake to interact with files stored outside its environment.

```sql
USE DB_NAME;
USE SCHEMA SCHEMA_NAME;
CREATE OR REPLACE STAGE parquet_unload_gcs_stage
URL = 'gcs://bucket'
storage_integration  = gcs_integration
FILE_FORMAT = parquet_unload_file_format
COMMENT = 'GCS Stage for the Snowflake external Parquet export';
```

### 8. Grant Permissions
Ensure that the Snowflake role used for unloading has the necessary privileges on all relevant Snowflake objects. You can create a role and assign permissions as needed.

```sql
CREATE ROLE <SF_ROLE>;
GRANT USAGE ON  FILE FORMAT <SF_PARQUET_FILE_FORMAT>  TO <SF_ROLE>;
GRANT USAGE ON  STAGE <SF_PARQUET_STAGE> TO <SF_ROLE>;
GRANT USAGE ON DATABASE <SF_DB> TO ROLE <SF_ROLE>;
GRANT USAGE ON SCHEMA <SF_SCHEMA> TO ROLE <SF_ROLE>;
GRANT SELECT ON ALL TABLES IN SCHEMA <SF_SCHEMA> TO ROLE <SF_ROLE>;
GRANT USAGE ON WAREHOUSE <SF_WAREHOUSE>  TO ROLE <SF_ROLE>;
GRANT ROLE <SF_ROLE> TO <SNOWFLAKE_USER>;
```

### 9. Snowflake Connection Setup

- Set up your **`~/.snowflake/connections.toml`** file with your Snowflake credentials

### 10. GCP Setup  

- Set up **GCP credentials** via `GOOGLE_APPLICATION_CREDENTIALS` environment variable
- Ensure your **BigQuery datasets** location matches your configuration

### 11. Environment Variables

Create a **`.env`** file in the project root:

```env
# Required
EXTERNAL_STAGE=DB_NAME.SCHEMA_NAME.parquet_unload_gcs_stage
PROJECT_ID=your-gcp-project
GCS_URI=gs://your-bucket

# Optional (with defaults)
CONFIG_FILE_PATH=config/tables.yml
SNOWFLAKE_CONNECTION_NAME=default
BIGQUERY_DATA_LOCATION=EU
BIGQUERY_DATASET_PREFIX=snowflake_
SAMPLE_LIMIT=100
LOGS_PATH=logs
```

### 12. Table Configuration

Edit **`config/tables.yml`** to specify what to migrate:

```yaml
# Migrate specific table
- database: PROD
  schema: ANALYTICS  
  table: sales_data

# Migrate entire schema (excluding temp tables)
- database: DEV
  schema: PUBLIC
  exclude_table_like:
    - 'TMP_%'
    - '%_TEMP'

# Include views as well
- database: PROD
  schema: REPORTING
  with_views: true
```

**Available options:**
- **`database`** *(required)* - Snowflake database name
- **`schema`** *(optional)* - Schema name (if omitted, all schemas)
- **`table`** *(optional)* - Specific table name
- **`exclude_schema_like`** *(optional)* - Schema patterns to exclude
- **`exclude_table_like`** *(optional)* - Table patterns to exclude  
- **`with_views`** *(optional)* - Include views (default: false)

## Usage

### Basic Commands

```bash
# Test your setup - shows detailed table info and queries (recommended first step)
python src/main.py --dry_run

# Run the migration
python src/main.py

# Interactive mode - ask permission for each table
python src/main.py --interactive

# Sample mode - migrate only 100 rows per table (for testing)
python src/main.py --sample

# Verbose mode - show cleaning and copy queries during execution
python src/main.py --verbose
```

### Dry Run Output
When using `--dry_run`, you'll see detailed information for each table:
```
Table 1/3: PROD.ANALYTICS.sales_data
============================================================
Database: PROD
Schema:   ANALYTICS
Table:    sales_data
Type:     BASE TABLE
Rows:     1,250,000

Generated Copy Query:
----------------------------------------
COPY INTO @my_stage/prod/analytics/sales_data/
FROM (
    SELECT
    "ID" AS "ID",
    "CUSTOMER_NAME" AS "CUSTOMER_NAME",
    "SALE_DATE" AS "SALE_DATE",
    CAST("CREATED_AT" AS STRING) AS "CREATED_AT"
FROM PROD.ANALYTICS.sales_data
)
FILE_FORMAT = (TYPE = PARQUET, SNAPPY_COMPRESSION = TRUE)
OVERWRITE = TRUE
HEADER = TRUE;
----------------------------------------

Table analysis saved to: logs/dry_mode_2024-03-15_14-30-25.yml
```

**Dry Run Analysis File:**
In addition to console output, dry run creates a timestamped YAML file with complete table information for future reference, analysis, or debugging. This file includes all table metadata, column details, and generated queries.

### Interactive Mode Options

When using `--interactive`, you'll be prompted for each table:

- Proceed with migration
- Skip to next table
- Edit cleaning query
- Edit copy query
- Abort migration

With `--verbose`, the cleaning and copy queries are shown before the prompt.

### BigQuery Table Loading (interactive)

Before loading into BigQuery, you can adjust table settings:

- Proceed with loading
- Skip to next table
- Edit schema
- Edit partition settings
- Edit cluster settings
- Abort migration

Note: If partition or cluster fields are provided without a custom schema, BigQuery schema autodetect is used. If you provide a custom schema, it takes precedence.

### On Failure (retry prompt)

- Retry
- Edit query and retry (for cleaning/COPY steps)
- Skip to next table

### Data Validation

Every table migration automatically validates that row counts match between Snowflake and BigQuery:

```
Processing table PROD.ANALYTICS.sales_data...
1,250,000 rows migrated successfully
```

If counts don't match, the migration fails with a clear error:
```
ERROR: Row count mismatch: Snowflake=1,250,000, BigQuery=1,248,532
```

Validation results appear in:
- **Console output** - Real-time feedback on validation status
- **Log files** - Detailed counts for each table in timestamped logs

### Verbose Mode

Use `--verbose` to see the actual cleaning and copy queries during execution:
- **In interactive mode**: Shows queries when asking for permission (edit options are available in all modes)
- **In non-interactive mode**: Shows queries before execution  
- **Helpful for**: Debugging, understanding what's happening, query verification, query fine-tuning

**Note:** Query editing (cleaning/copy) is now available in **all interactive mode sessions**, regardless of verbose setting.

### Query Editing

When a Snowflake cleaning or COPY query fails, you can choose to edit it before retrying:
- The current query opens in your default editor
- Make your changes and save the file
- The updated query will be used for the retry

**Editor Selection:**
- Uses `$EDITOR` environment variable if set
- **Windows**: Defaults to `notepad`  
- **macOS/Linux**: Tries `code` → `vim` → `nano` → `vi` (first available)

Sure thing! Here's the streamlined version with just the query and the key implementation notes:

### Query Generation Choices

```sql
COPY INTO @my_stage/database/schema/table/
FROM (
    SELECT
        "ID" AS "ID",
        "NAME" AS "NAME",
        CAST("TIMESTAMP_COL" AS STRING) AS "TIMESTAMP_COL",
        "user.profile.email" AS "user_profile_email"
    FROM DATABASE.SCHEMA.TABLE
    LIMIT 100
)
FILE_FORMAT = (TYPE = PARQUET, SNAPPY_COMPRESSION = TRUE)
OVERWRITE = TRUE
HEADER = TRUE;
```

To ensure compatibility and avoid runtime errors, a few transformations are automatically applied during query generation:
- **Timestamp casting**:  
  `TIMESTAMP_TZ` and `TIMESTAMP_LTZ` types can cause unload errors, so they're cast to `STRING` by default.  
  _You can override this behavior in interactive+verbose mode if needed.
- **Smart column aliasing**:  
  Column names are normalized using BigQuery V2 rules directly in COPY queries:
  - Special characters (dots, spaces, hyphens) → underscores: `user.profile.email` → `user_profile_email`
  - Uppercase → lowercase: `CUSTOMER_ID` → `customer_id`  
  - Invalid prefixes handled: names starting with numbers get `_` prefix
  - Reserved prefixes avoided: `_TABLE_data` → `_table_data_`

### BigQuery V2 Normalization

The tool applies BigQuery's `column_name_character_map="V2"` normalization throughout:
- **COPY query generation**: Column aliases are V2-normalized in Snowflake COPY statements
- **Schema inference**: When building BigQuery table schemas from Snowflake metadata
- **Interactive editing**: Column names displayed in partition/cluster selection use V2 names
- **Deduplication**: Handles conflicts when multiple columns normalize to the same name

This ensures consistency between Snowflake export column names and BigQuery table schemas, eliminating load-time column name mismatches.

## Output & Logging

The tool creates different types of timestamped log files in the `logs/` directory:

### Migration Mode Files:
- **`succeeded_tables_<timestamp>.yml`** → Successfully migrated tables with execution details
- **`failed_tables_<timestamp>.yml`** → Failed/skipped tables with error messages and stack traces

### Dry Run Mode Files:
- **`dry_mode_<timestamp>.yml`** → Complete table analysis including:
  - Table metadata (database, schema, table, type, row counts)
  - Column definitions and data types
  - Generated cleaning query (REMOVE commands) and copy query (COPY INTO statements)
  - All queries beautifully formatted with proper indentation
  - All query parameters and configuration

**File Organization:**
- **Dry run**: Only creates analysis files (no empty migration result files)
- **Migration**: Only creates execution result files (no analysis files)
- **Timestamps**: All files use `YYYY-MM-DD_HH-MM-SS` format for easy sorting

**Pro tips:**
- Copy failed tables from migration logs back to `tables.yml` to retry failures
- Use dry run files for migration planning, capacity analysis, and debugging
- Share dry run files with team members for migration review and approval

## Testing

Quick tests to verify everything works:

```bash
python -m pytest tests/ -v
```

## If things get stuck

Sometimes a query runs way longer than expected. Here's how to kill it:

**Snowflake**: Go to History tab in web UI and click Cancel, or run `SELECT SYSTEM$CANCEL_ALL_QUERIES();`

**BigQuery**: Go to Job history in BigQuery console and click Cancel job, or `bq cancel <job_id>`
