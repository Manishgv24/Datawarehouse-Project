/*

=================================================================================
Create database schemas
=================================================================================


SCRIPT PURPOSE :

                This scripts checks for the database 'Datawarehouse'. It checks the system databases, if exists it will drop it and
recreate the database. Additionally it set up the three schemas named 'Bronze', 'Silver' and 'Gold'.


WARNING :
          Running this script will permamnetly delete all the data with the databse Datawarehouse.
		  Run it carefully. Proceed with caution and ensure you have proper backup.
*/




USE master;

--DROP and recreate the database 'Datawarehouse' database

IF EXISTS (SELECT 1 FROM sys.databases WHERE name= 'Datawarehouse')
BEGIN
	ALTER DATABASE datawarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE Datawarehouse;
END;


-- Creating the database--

CREATE DATABASE Datawarehouse

USE Datawarehouse

--Creating the SCHEMAS for the project--

CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO
