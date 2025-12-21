-- Configuration table for managing Dataverse entity extraction
-- This is optional - you can also define entities directly in Python code

-- Create schema if it doesn't exist
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'dbo')
BEGIN
    EXEC('CREATE SCHEMA dbo')
END
GO

-- Create SourceProperties table for entity configuration
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'SourceProperties' AND schema_id = SCHEMA_ID('dbo'))
BEGIN
    CREATE TABLE [dbo].[SourceProperties] (
        [SourceName] NVARCHAR(100) NOT NULL PRIMARY KEY,  -- Dataverse entity name (e.g., 'accounts', 'contacts')
        [Active] BIT NOT NULL DEFAULT 1,                  -- Enable/disable extraction for this entity
        [Order] INT NOT NULL DEFAULT 10,                  -- Extraction order (lower numbers first)
        [Last_FullLoad] DATETIME NULL,                    -- Timestamp of last successful extraction
        [Columns] NVARCHAR(MAX) NULL,                     -- Comma-separated list of columns to extract (NULL = all columns)
        [Where] NVARCHAR(MAX) NULL,                       -- OData filter clause (e.g., 'statecode eq 0')
        [Description] NVARCHAR(500) NULL,                 -- Optional description
        [CreatedDate] DATETIME NOT NULL DEFAULT GETDATE(),
        [ModifiedDate] DATETIME NOT NULL DEFAULT GETDATE()
    )
    
    PRINT 'Created table dbo.SourceProperties'
END
ELSE
BEGIN
    PRINT 'Table dbo.SourceProperties already exists'
END
GO

-- Insert example configurations
IF NOT EXISTS (SELECT * FROM [dbo].[SourceProperties] WHERE SourceName = 'accounts')
BEGIN
    INSERT INTO [dbo].[SourceProperties] (SourceName, Active, [Order], Columns, [Where], Description)
    VALUES 
        ('accounts', 1, 1, 'accountid, name, emailaddress1, telephone1', NULL, 'Customer accounts'),
        ('contacts', 1, 2, 'contactid, firstname, lastname, emailaddress1', 'statecode eq 0', 'Active contacts only'),
        ('opportunities', 1, 10, NULL, 'estimatedvalue gt 10000', 'High-value opportunities'),
        ('orders', 0, 10, 'orderid, name, totalamount, statuscode', NULL, 'Sales orders (disabled)')
    
    PRINT 'Inserted example configurations'
END
GO

-- Create index for faster queries
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_SourceProperties_Active_Order' AND object_id = OBJECT_ID('dbo.SourceProperties'))
BEGIN
    CREATE NONCLUSTERED INDEX IX_SourceProperties_Active_Order 
    ON [dbo].[SourceProperties] ([Active], [Order])
    INCLUDE ([SourceName], [Columns], [Where])
    
    PRINT 'Created index IX_SourceProperties_Active_Order'
END
GO

-- View active entities
SELECT 
    SourceName,
    [Order],
    Columns,
    [Where],
    Last_FullLoad,
    Description
FROM [dbo].[SourceProperties]
WHERE Active = 1
ORDER BY [Order], SourceName
GO

PRINT 'Setup complete!'
PRINT ''
PRINT 'Usage in Python:'
PRINT '  1. Query this table to get entities to extract'
PRINT '  2. Pass to DataverseExtractor.main(entities)'
PRINT '  3. Update Last_FullLoad after successful extraction'
