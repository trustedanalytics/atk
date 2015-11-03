--
--  Copyright (c) 2015 Intel Corporation 
--
--  Licensed under the Apache License, Version 2.0 (the "License");
--  you may not use this file except in compliance with the License.
--  You may obtain a copy of the License at
--
--       http://www.apache.org/licenses/LICENSE-2.0
--
--  Unless required by applicable law or agreed to in writing, software
--  distributed under the License is distributed on an "AS IS" BASIS,
--  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
--  See the License for the specific language governing permissions and
--  limitations under the License.
--

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

