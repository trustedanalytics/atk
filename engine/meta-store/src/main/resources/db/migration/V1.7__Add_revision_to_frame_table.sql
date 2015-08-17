--
-- PostgreSQL Schema Migration
--
-- See FlywayDB.org documentation to see how these changes are applied
--
-- !!! DO NOT MODIFY THIS SCRIPT !!!
--

-- Each time the physical frame is modified, update the revision

ALTER TABLE frame ADD COLUMN revision int;

UPDATE frame SET revision = 1;
