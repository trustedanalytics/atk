--
-- PostgreSQL Schema Migration
--
-- See FlywayDB.org documentation to see how these changes are applied
--
-- !!! DO NOT MODIFY THIS SCRIPT !!!
--

--
-- "Seamless Graph" is a graph that provides a "seamless user experience" between graphs and frames.
-- The same data can be treated as frames one moment and as a graph the next without any import/export.
--

ALTER TABLE frame ADD COLUMN graph_id bigint;

ALTER TABLE ONLY frame ADD CONSTRAINT graph_frame_id FOREIGN KEY (graph_id) REFERENCES graph(graph_id);

ALTER TABLE graph ADD COLUMN storage_format CHARACTER VARYING(32);
UPDATE graph SET storage_format = 'hbase/titan';
ALTER TABLE graph ALTER COLUMN storage_format SET NOT NULL;



ALTER TABLE graph ADD COLUMN id_counter BIGINT;

