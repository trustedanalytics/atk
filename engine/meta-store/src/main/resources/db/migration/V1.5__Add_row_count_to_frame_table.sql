--
-- PostgreSQL Schema Migration
--
-- See FlywayDB.org documentation to see how these changes are applied
--
-- !!! DO NOT MODIFY THIS SCRIPT !!!
--

-- When the user loads data into a frame, parse errors go into a separate frame referenced by error_frame_id

ALTER TABLE frame ADD COLUMN row_count bigint;
