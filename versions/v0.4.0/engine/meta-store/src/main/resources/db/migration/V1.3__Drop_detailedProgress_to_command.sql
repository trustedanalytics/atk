--
-- PostgreSQL Schema Migration
--
-- See FlywayDB.org documentation to see how these changes are applied
--
-- !!! DO NOT MODIFY THIS SCRIPT !!!
--


/**
 * drop column detailedProgress from command table
 */
ALTER TABLE command DROP COLUMN detailed_progress;