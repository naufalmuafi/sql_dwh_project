/*
=============================================================
Create Database and Schemas
=============================================================
Script Purpose:
    This script creates a new database named 'DWH' after checking if it already exists. 
    If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas 
    within the database: 'bronze', 'silver', and 'gold'.
	
WARNING:
    Running this script will drop the entire 'DWH' database if it exists. 
    All data in the database will be permanently deleted. Proceed with caution 
    and ensure you have proper backups before running this script.
*/

-- Drop and recreate the 'DWH' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DWH')
BEGIN
    ALTER DATABASE DWH SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DWH;
END;

-- Create the Database
CREATE DATABASE DWH;

-- Create Schemas
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;