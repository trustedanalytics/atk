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

ALTER TABLE graph ADD COLUMN element_id_names text;

