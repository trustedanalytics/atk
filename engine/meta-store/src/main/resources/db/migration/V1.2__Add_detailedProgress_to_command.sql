--
-- PostgreSQL Schema Migration
--
-- See FlywayDB.org documentation to see how these changes are applied
--
-- !!! DO NOT MODIFY THIS SCRIPT !!!
--


/**
 * Add the column detailedProgress to command table
 * This column is used to display extra details of command progress
 */
ALTER TABLE command ADD COLUMN detailed_progress text NOT NULL DEFAULT '[]';


-- Fix: someone else already dropped this column

ALTER TABLE frame DROP COLUMN uri;