USE gold_db;
GO

CREATE OR ALTER PROCEDURE SqlServerlessView_Gold @viewName NVARCHAR(100)
AS
BEGIN
    DECLARE @statement NVARCHAR(MAX)
    SET @statement = N'
        CREATE OR ALTER VIEW ' + QUOTENAME(@viewName) + ' AS
        BEGIN 
            SELECT *
            FROM OPENROWSET(
                BULK ''jvstorageservices.dfs.core.windows.net/gold/Sales/' + @viewName + '/'',
                FORMAT = ''DELTA''
            )
        END
    '
    EXEC (@statement)
END
GO
