--
-- PostgreSQL Schema Migration
--
-- See FlywayDB.org documentation to see how these changes are applied
--
-- !!! DO NOT MODIFY THIS SCRIPT !!!
--


/**
 * Delete existing Titan Graphs from meta_store since Titan is no longer
 * supported
 */
DELETE FROM graph WHERE storage_format = 'hbase/titan';
