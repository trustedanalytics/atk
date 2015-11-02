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

