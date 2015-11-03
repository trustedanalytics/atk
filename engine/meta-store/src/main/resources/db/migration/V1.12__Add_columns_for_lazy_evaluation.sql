--
-- PostgreSQL Schema Migration
--
-- This file was generated from pg_dump with modifications
-- See FlywayDB.org documentation to see how these changes are applied
--
-- !!! DO NOT MODIFY THIS SCRIPT !!!
--

-- Add columns to frames:

ALTER TABLE frame ADD COLUMN command_id bigint NULL;

ALTER TABLE frame ADD COLUMN parent_frame_id bigint NULL;

ALTER TABLE frame ADD COLUMN materialized_start TIMESTAMP WITHOUT TIME ZONE NULL;

ALTER TABLE frame ADD COLUMN materialized_end TIMESTAMP WITHOUT TIME ZONE NULL;

ALTER TABLE frame ADD COLUMN storage_format TEXT NULL;

ALTER TABLE frame ADD COLUMN storage_uri TEXT NULL;

UPDATE frame SET storage_format = 'file/parquet', storage_uri = frame.frame_id || '/rev' || frame.revision;

ALTER TABLE frame DROP COLUMN revision;

-- Add correlation id to commands:
ALTER TABLE command ADD COLUMN correlation VARCHAR(50) NOT NULL DEFAULT '';



