--
-- PostgreSQL Schema Migration
--
-- See FlywayDB.org documentation to see how these changes are applied
--
-- !!! DO NOT MODIFY THIS SCRIPT !!!
--

UPDATE graph SET storage_format = 'atk/frame' WHERE storage_format = 'ia/frame';
