--
-- PostgreSQL Schema Migration
--
-- See FlywayDB.org documentation to see how these changes are applied
--
-- !!! DO NOT MODIFY THIS SCRIPT !!!
--

--
-- update storage to iat_graph_<id> for all hbase/titan backed graphs
--

UPDATE graph SET storage = 'iat_graph_' || graph.name WHERE graph.storage_format = 'hbase/titan'
