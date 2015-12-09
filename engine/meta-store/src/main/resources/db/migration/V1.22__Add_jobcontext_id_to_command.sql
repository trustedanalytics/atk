--
-- PostgreSQL Schema Migration
--
-- See FlywayDB.org documentation to see how these changes are applied
--
-- !!! DO NOT MODIFY THIS SCRIPT !!!
--


/**
 * Add the column jobcontext_id to command table
 * This column is used to display the associated jobcontext_id for every command
 */
ALTER TABLE command ADD COLUMN jobcontext_id bigint NULL;
ALTER TABLE command ADD CONSTRAINT JOBCONTEXT_ID_CONSTRAINT FOREIGN KEY (jobcontext_id) REFERENCES jobcontext(jobcontext_id);
