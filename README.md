# Dataverse to SQL Extract Pipeline

Production-ready Python script for extracting data from Microsoft Dataverse and loading it into SQL Server. Features connection pooling, circuit breakers, rate limiting, and parallel processing for efficient and reliable data extraction.

## Features

- **Parallel Processing**: Multi-threaded extraction with configurable workers
- **Connection Pooling**: Efficient SQL connection management with SQLAlchemy
- **Circuit Breaker Pattern**: Automatic failure handling per entity
- **Rate Limiting**: Respects API rate limits with configurable intervals
- **Retry Logic**: Automatic retries with exponential backoff
- **Timeout Management**: Configurable timeouts at entity and group levels
- **Automatic Table Creation**: Creates SQL tables dynamically based on Dataverse schema
- **Column Filtering**: Extract specific columns or all columns per entity
- **Where Clause Support**: Filter data at source using OData filters
- **Comprehensive Logging**: Detailed logging to file and console

## Requirements

- Python 3.11+
- Microsoft SQL Server with ODBC Driver 17+
- Azure AD application with Dataverse API permissions
- Access to Microsoft Dataverse environment

## Installation

```bash
# Clone repository
git clone https://github.com/Ajollyworld79/data-pipeline.git
cd data-pipeline

# Install dependencies
pip install -r requirements.txt
```

## Configuration

Set the following environment variables:

### Required Variables

```bash
# Azure AD Configuration
export TENANT_ID="your-tenant-id"
export CLIENT_ID="your-client-id"
export CLIENT_SECRET="your-client-secret"

# Dataverse Configuration
export DATAVERSE_URL="https://yourorg.crm.dynamics.com"

# Database Configuration
export DB_CONNECTION_STRING="mssql+pyodbc://user:password@server/database?driver=ODBC+Driver+17+for+SQL+Server"
```

### Optional Variables

```bash
# Performance Tuning
export MAX_WORKERS="3"                    # Parallel workers (default: 3)
export PAGE_SIZE="5000"                   # Records per page (default: 5000)
export RATE_LIMIT_INTERVAL="0.3"          # Seconds between API calls (default: 0.3)

# Timeout Configuration
export GROUP_TIMEOUT_SECONDS="10800"      # Total timeout for all entities (default: 3 hours)
export ENTITY_TIMEOUT_SECONDS="7200"      # Timeout per entity (default: 2 hours)

# Database Configuration
export DB_SCHEMA="dbo"                    # SQL schema for tables (default: dbo)
export DB_POOL_SIZE="5"                   # Connection pool size (default: 5)
export DB_MAX_OVERFLOW="10"               # Max additional connections (default: 10)
export DB_POOL_TIMEOUT="30"               # Connection timeout in seconds (default: 30)
export DB_POOL_RECYCLE="3600"             # Recycle connections after seconds (default: 1 hour)
```

## Usage

The script supports two modes of operation:

### Option 1: SQL Configuration Table (Recommended)

Use a SQL table to manage which entities to extract. This is the recommended approach for production use.

**Step 1: Create configuration table**

```bash
# Run the setup script to create SourceProperties table
sqlcmd -S your-server -d your-database -i setup_config_table.sql
```

This creates a `SourceProperties` table with example data:

| SourceName | Active | Order | Columns | Where | Last_FullLoad |
|------------|--------|-------|---------|-------|---------------|
| accounts | 1 | 1 | accountid, name, emailaddress1 | NULL | NULL |
| contacts | 1 | 2 | contactid, firstname, lastname | statecode eq 0 | NULL |

**Step 2: Run extraction**

```bash
python dataverse_extract.py
```

The script will:
- Query `SourceProperties` for active entities
- Extract data from Dataverse
- Load into SQL tables (creates tables automatically if needed)
- Update `Last_FullLoad` timestamp after successful extraction
- Skip entities where `Last_FullLoad` is today (prevents duplicate runs)

**Managing entities in SQL:**

```sql
-- Add new entity
INSERT INTO [dbo].[SourceProperties] (SourceName, Active, [Order], Columns, [Where])
VALUES ('opportunities', 1, 10, 'opportunityid, name, estimatedvalue', 'statecode eq 0')

-- Disable an entity
UPDATE [dbo].[SourceProperties] SET Active = 0 WHERE SourceName = 'orders'

-- Reset Last_FullLoad to force re-extraction
UPDATE [dbo].[SourceProperties] SET Last_FullLoad = NULL WHERE SourceName = 'accounts'

-- View active entities
SELECT SourceName, [Order], Columns, [Where], Last_FullLoad
FROM [dbo].[SourceProperties]
WHERE Active = 1
ORDER BY [Order]
```

### Option 2: Hardcoded Entities

For testing or one-off extractions, you can hardcode entities directly in the script.

**Edit `dataverse_extract.py`:**

```python
if __name__ == "__main__":
    # Define entities directly
    entities = [
        {
            "entity_name": "accounts",
            "columns": ["accountid", "name", "emailaddress1"],
            "where_clause": None
        },
        {
            "entity_name": "contacts",
            "columns": None,  # Extract all columns
            "where_clause": "statecode eq 0"  # Only active contacts
        }
    ]
    
    with DataverseExtractor() as extractor:
        exit_code = extractor.main(entities)
    
    os._exit(exit_code)
```

**Run:**

```bash
python dataverse_extract.py
```

## OData Filter Examples

The script supports OData filter syntax for the `where_clause` parameter:

```python
# Equals
"statecode eq 0"

# Greater than
"estimatedvalue gt 10000"

# Date comparison
"createdon ge 2024-01-01"

# Logical operators
"statecode eq 0 and estimatedvalue gt 10000"

# String operations
"contains(name, 'Contoso')"

# In operator
"statecode in (0, 1)"
```

## Azure AD Application Setup

1. **Register Application** in Azure AD
2. **Add API Permissions**:
   - Dynamics CRM API
   - user_impersonation (delegated)
   - OR create application permissions for service principal
3. **Create Client Secret**
4. **Grant Admin Consent** for the permissions
5. **Assign Application User** in Dataverse with appropriate security role

## Database Setup

The script automatically creates tables in SQL Server if they don't exist. All columns are created as `NVARCHAR(100)` with an additional `AuditDate` timestamp column.

### Example Table Structure

```sql
CREATE TABLE dbo.accounts (
    accountid NVARCHAR(100),
    name NVARCHAR(100),
    emailaddress1 NVARCHAR(100),
    AuditDate DATETIME
)
```

## Logging

Logs are written to both console and `dataverse_extract.log` file:

```
2025-12-21 20:00:00 - INFO - Starting data retrieval for: accounts (attempt 1/3)
2025-12-21 20:00:05 - INFO - Fetching all columns for accounts
2025-12-21 20:00:10 - INFO - Pagination complete for accounts. Total: 5000 records
2025-12-21 20:00:15 - INFO - accounts: Uploaded chunk 1 (5000 records, total: 5000)
2025-12-21 20:00:20 - INFO - Transaction completed for accounts. Total records: 5000
2025-12-21 20:00:20 - INFO - Data upload completed for table: accounts
```

## Performance Tuning

### For Large Datasets

```bash
export MAX_WORKERS="5"           # More parallel workers
export PAGE_SIZE="5000"          # Standard page size
export RATE_LIMIT_INTERVAL="0.2" # Faster API calls (if not rate limited)
```

### For Rate-Limited Environments

```bash
export MAX_WORKERS="2"           # Fewer parallel workers
export PAGE_SIZE="2500"          # Smaller pages
export RATE_LIMIT_INTERVAL="0.5" # Slower API calls
```

### For Slow Networks

```bash
export ENTITY_TIMEOUT_SECONDS="14400"  # 4 hours per entity
export GROUP_TIMEOUT_SECONDS="28800"   # 8 hours total
```

## Error Handling

The script includes comprehensive error handling:

- **Circuit Breaker**: Opens after 5 consecutive failures per entity
- **Retry Logic**: Retries failed requests 3 times with exponential backoff
- **Token Refresh**: Automatically refreshes expired Azure AD tokens
- **Rate Limit Handling**: Respects `Retry-After` headers from API
- **Timeout Management**: Cancels long-running operations gracefully

## Troubleshooting

### Common Issues

**Authentication Error (401)**:
- Verify `TENANT_ID`, `CLIENT_ID`, and `CLIENT_SECRET`
- Check Azure AD application permissions
- Ensure application user exists in Dataverse

**Rate Limiting (429)**:
- Increase `RATE_LIMIT_INTERVAL`
- Reduce `MAX_WORKERS`
- Reduce `PAGE_SIZE`

**Database Connection Error**:
- Verify `DB_CONNECTION_STRING`
- Check ODBC Driver 17 is installed
- Test database connectivity with `sqlcmd` or similar tool

**Timeout Errors**:
- Increase `ENTITY_TIMEOUT_SECONDS` and `GROUP_TIMEOUT_SECONDS`
- Reduce `MAX_WORKERS` to avoid overloading
- Check network connectivity

## Project Structure

```
data-pipeline/
├── dataverse_extract.py          # Main extraction script
├── setup_config_table.sql        # SQL script to create SourceProperties table
├── Module/
│   ├── Connection_pool.py        # SQLAlchemy connection pooling
│   └── Functions.py              # Utility functions (timeout decorator)
├── requirements.txt              # Python dependencies
└── README.md                     # This file
```

## License

MIT License - See LICENSE file

## Author

Gustav Wind Christensen
- Email: guch79@gmail.com
- LinkedIn: [gustav-wind-christensen](https://www.linkedin.com/in/gustav-wind-christensen-b61a0330)
- GitHub: [Ajollyworld79](https://github.com/Ajollyworld79)

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test thoroughly
5. Submit a pull request

## Support

For issues or questions, please open an issue on GitHub or contact the author directly.
