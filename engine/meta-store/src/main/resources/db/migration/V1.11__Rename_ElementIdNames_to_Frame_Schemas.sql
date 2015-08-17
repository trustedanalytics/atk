--
-- PostgreSQL Schema Migration
--
-- See FlywayDB.org documentation to see how these changes are applied
--
-- !!! DO NOT MODIFY THIS SCRIPT !!!
--

--
-- element_id_names contains the list of all of the unique id columns for the ids in a graph.
--

UPDATE graph SET element_id_names = null;
ALTER TABLE graph RENAME COLUMN element_id_names TO frame_schemas;

