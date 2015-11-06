--
-- PostgreSQL Schema Migration
--
-- See FlywayDB.org documentation to see how these changes are applied
--
-- !!! DO NOT MODIFY THIS SCRIPT !!!
--

--
-- storage_uri is the uri for model data storage
--

ALTER TABLE model ADD COLUMN storage_uri text;

