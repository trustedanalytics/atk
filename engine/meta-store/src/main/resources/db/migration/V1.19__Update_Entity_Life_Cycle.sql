--
-- PostgreSQL Schema Migration
--
-- This file was generated from pg_dump with modifications
-- See FlywayDB.org documentation to see how these changes are applied
--
-- !!! DO NOT MODIFY THIS SCRIPT !!!
--

--
-- update the names and descriptions of for the entity life cycle states
--


UPDATE status
    SET name= 'ACTIVE', description = 'Available to the user'
    WHERE status_id = 1;

UPDATE status
    SET description = 'No longer available to the user',
     name = 'DROPPED'
    WHERE status_id = 2;

UPDATE status
    SET description = 'Not available and data has been deleted',
     name = 'FINALIZED'
    WHERE status_id = 3;

