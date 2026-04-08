-- Data Pipeline POC - Schema creation
-- Four logical layers: raw, staging, curated, ops

CREATE DATABASE IF NOT EXISTS exchange;
USE exchange;

-- We use prefixed table names instead of separate databases
-- to keep the docker setup simple: raw_*, staging_*, curated_*, ops_*
-- The schemas below are conceptual separations.

-- Create a dedicated user for DBM
CREATE USER IF NOT EXISTS 'datadog'@'%' IDENTIFIED BY 'datadog';
GRANT REPLICATION CLIENT ON *.* TO 'datadog'@'%';
GRANT PROCESS ON *.* TO 'datadog'@'%';
GRANT SELECT ON performance_schema.* TO 'datadog'@'%';
GRANT SELECT ON exchange.* TO 'datadog'@'%';
GRANT SELECT ON mysql.* TO 'datadog'@'%';

-- DBM explain plans
CREATE SCHEMA IF NOT EXISTS datadog;
DELIMITER $$
CREATE PROCEDURE IF NOT EXISTS datadog.explain_statement(IN query TEXT)
    SQL SECURITY DEFINER
BEGIN
    SET @explain := CONCAT('EXPLAIN FORMAT=json ', query);
    PREPARE stmt FROM @explain;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END $$
DELIMITER ;
GRANT EXECUTE ON PROCEDURE datadog.explain_statement TO 'datadog'@'%';
