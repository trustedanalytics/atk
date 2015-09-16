--
-- PostgreSQL Schema Migration
--
-- See FlywayDB.org documentation to see how these changes are applied
--
-- !!! DO NOT MODIFY THIS SCRIPT !!!
--

-- When the user loads data into a frame, parse errors go into a separate frame referenced by error_frame_id

ALTER TABLE frame ADD COLUMN error_frame_id bigint;

ALTER TABLE ONLY frame
    ADD CONSTRAINT frame_error_frame_id FOREIGN KEY (error_frame_id) REFERENCES frame(frame_id);