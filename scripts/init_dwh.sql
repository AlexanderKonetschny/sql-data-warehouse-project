/*
======================================================================================
Create Database
======================================================================================
Script Purpose:
    This script creates a new database named 'dwh_project' after checking if it already exists. 
    If the database exists, it is dropped and recreated. At the end, a table ist created to log
    the loading processes.
WARNING:
    Running this script will drop the entire 'dwh_project' database if it exists. 
    All data in the database will be permanently deleted. Proceed with caution 
    and ensure you have proper backups before running this script.
======================================================================================
*/

-- delete database if exists
DROP DATABASE IF EXISTS dwh_project;

-- create database
CREATE DATABASE dwh_project; 

-- select database
USE dwh_project; 

-- create table for logging loading process
CREATE TABLE IF NOT EXISTS loading_log (
    log_id 					INT AUTO_INCREMENT PRIMARY KEY,
    table_name 				VARCHAR(255),
    load_start				DATETIME,
    load_end				DATETIME,
    load_duration_seconds 	INT
);
