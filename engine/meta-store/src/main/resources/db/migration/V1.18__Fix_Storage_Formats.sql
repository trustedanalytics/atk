--
-- PostgreSQL Schema Migration
--
-- See FlywayDB.org documentation to see how these changes are applied
--
-- !!! DO NOT MODIFY THIS SCRIPT !!!
--

UPDATE graph SET storage_format = 'atk/frame' WHERE storage_format = 'ia/frame';
UPDATE graph SET storage_format = 'atk/model' WHERE storage_format = 'ia/model';
UPDATE graph SET storage_format = 'atk/graph' WHERE storage_format = 'ia/graph';

