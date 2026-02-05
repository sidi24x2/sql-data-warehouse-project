/*
	==============================================================
	CREATE DATABASE AND SCHEMA 
	==============================================================

	SCRIPT PURPOSE :
	This query will create a database named 'data_warehouse'
	if there is already a database with similar name it'll first 
	drop the database then recreate it. Additionally it'll create
	three schema within the database : 'bronze' , 'silver' , 'gold'

	WARNING : 
	Running this script will delete the data_warehouse. Running
	this script will delete all the data from the database 'data_warehouse'
*/

USE master
GO

-- Checks for existing database 
IF EXISTS (SELECT 1 FROM SYS.DATABASES WHERE NAME =  'data_warehouse') 
	DROP DATABASE data_warehouse;
GO

-- Creating Database 
CREATE DATABASE data_warehouse ;
GO

USE data_warehouse;
GO 

-- Creating Schema 
CREATE SCHEMA bronze;
GO 
CREATE SCHEMA silver;
GO 
CREATE SCHEMA gold;
