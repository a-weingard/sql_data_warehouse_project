/*
=============================================================
Database and Schema Creation
=============================================================
Purpose of this Script:
    This script creates a new database named 'DataWarehouse'.
    If the database already exists, it will be dropped and recreated.
    The script also creates a set of 3 schemas within the database 'bronze', 'silver', and 'gold'.

WARNING/IMPORTANT NOTICE:
    Executing this script will permanently delete the existing 'DataWarehouse' database along with all its data.
    Ensure you have valid backups before running this script and proceed with caution.
*/

-- CREATE DATABASE 'DataWarehouse'

USE MASTER;

-- Drp and recreate the 'Datawarehouse' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehouse;
END;

-- Create the 'DataWarehouse' Database
CREATE DATABASE DataWarehouse;

USE DataWarehouse;

-- Create Schemas
CREATE SCHEMA bronze;
CREATE SCHEMA silver;
CREATE SCHEMA gold;
