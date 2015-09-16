--
-- PostgreSQL Schema Migration
--
-- See FlywayDB.org documentation to see how these changes are applied
--
-- !!! DO NOT MODIFY THIS SCRIPT !!!
--


/**
 * Add the column progress to command table.
 * This is used to track the amount of progress that a command will report to the user.
 */

ALTER TABLE command ADD COLUMN progress text;