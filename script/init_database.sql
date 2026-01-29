/*
====================================
Create Database & Schema
====================================
Script Purpose: 
	This script creates as new database named 'DataWarehouse' after checking if it already exists.
	If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas 
	within the database : 'bronze' , 'silver' , gold'.

WARNING: 
	Running this script will drop the entire 'DataWarehouse' database if it exists.
	All the data of the database will be permanently deleted. Proceed with caution 
	and ensure you have proper backups before runnung this script.
*/

USE master;
GO 

-- Drop and Recreate the 'DatawareHouse' database
IF EXISTS(SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse') 
BEGIN 
	ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DataWarehouse;
END;
GO

-- Create the 'DataWarehouse' database 
CREATE DATABASE DataWarehouse
GO

USE DataWarehouse;
GO

-- CREATING SCHEMAS 

CREATE SCHEMA bronze 
GO
CREATE SCHEMA silver 
GO
CREATE SCHEMA gold
GO
