/* Creating Database and Schemas for Medallion Architecture

If the database exists, it is dropped and recreated. Creates three schemas: bronze, silver and gold
  
NOTE:
Running the script will drop the entire database, and if it exists, all data will be permanently deleted
*/

USE master;
GO

IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
	ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DataWarehouse;
END
GO

CREATE DATABASE DataWarehouse;
GO

USE DataWarehouse;

CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO



