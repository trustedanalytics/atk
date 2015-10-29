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
-- Name: garbage_collection; Type TABLE; Schema: public, owner: metastore Tablespace:
--
CREATE TABLE garbage_collection (
  garbage_collection_id bigint PRIMARY KEY,
  hostname varchar(255) NOT NULL,
  process_id bigint NOT NULL,
  start_time timestamp without time zone  NOT NULL,
  end_time  timestamp without time zone ,
  created_on timestamp without time zone NOT NULL,
  modified_on timestamp without time zone NOT NULL
);


--
-- Name: garbage_collection_garbage_collection_id_seq; Type: SEQUENCE; Schema: public; Owner: metastore
--
CREATE SEQUENCE garbage_collection_garbage_collection_id_seq
START WITH 1
INCREMENT BY 1
NO MAXVALUE
NO MINVALUE
CACHE 1;

--
-- Name: garbage_collection_garbage_collection_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: metastore
--

ALTER SEQUENCE garbage_collection_garbage_collection_id_seq OWNED BY garbage_collection.garbage_collection_id;


--
-- Name: garbage_collection_garbage_collection_id_seq; Type: SEQUENCE SET; Schema: public; Owner: metastore
--

SELECT pg_catalog.setval('garbage_collection_garbage_collection_id_seq', 1, false);

--
-- Name: garbage_collection_id; Type: DEFAULT; Schema: public; Owner: metastore
--

ALTER TABLE ONLY garbage_collection ALTER COLUMN garbage_collection_id SET DEFAULT nextval('garbage_collection_garbage_collection_id_seq'::regclass);

--
-- Name: garbage_collection_entry; Type TABLE; Schema: public, owner: metastore Tablespace:
--
CREATE TABLE garbage_collection_entry (
  garbage_collection_entry_id bigint PRIMARY KEY,
  garbage_collection_id bigint NOT NULL,
  description text NOT NULL,
  start_time  timestamp without time zone  NOT NULL,
  end_time  timestamp without time zone ,
  created_on timestamp without time zone NOT NULL,
  modified_on timestamp without time zone NOT NULL
);


--
-- Name: garbage_collection_entry_garbage_collection_entry_id_seq; Type: SEQUENCE; Schema: public; Owner: metastore
--
CREATE SEQUENCE garbage_collection_entry_garbage_collection_entry_id_seq
START WITH 1
INCREMENT BY 1
NO MAXVALUE
NO MINVALUE
CACHE 1;

--
-- Name: garbage_collection_entry_garbage_collection_entry_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: metastore
--

ALTER SEQUENCE garbage_collection_entry_garbage_collection_entry_id_seq OWNED BY garbage_collection.garbage_collection_id;


--
-- Name: garbage_collection_garbage_collection_id_seq; Type: SEQUENCE SET; Schema: public; Owner: metastore
--

SELECT pg_catalog.setval('garbage_collection_entry_garbage_collection_entry_id_seq', 1, false);

--
-- Name: garbage_collection_entry_id; Type: DEFAULT; Schema: public; Owner: metastore
--

ALTER TABLE ONLY garbage_collection_entry ALTER COLUMN garbage_collection_entry_id SET DEFAULT nextval('garbage_collection_entry_garbage_collection_entry_id_seq'::regclass);

--
-- Name: garbage_collection_id; Type: FK CONSTRAINT; Schema: public; Owner: metastore
--

ALTER TABLE ONLY garbage_collection_entry
ADD CONSTRAINT garbage_collection_entry_garbage_collection_id FOREIGN KEY (garbage_collection_id) REFERENCES garbage_collection(garbage_collection_id);


--
-- Data for Name: status; Type: TABLE DATA; Schema: public; Owner: metastore
--
INSERT INTO status (status_id, name, description, created_on, modified_on) VALUES
  (6, 'LIVE', 'Active and used.', now(), now()),
  (7, 'WEAKLY_LIVE', 'Active but unused. The data on disk may be deleted but can be recreated', now(), now()),
  (8, 'DEAD', 'INACTIVE AND UNUSED, the data on disk will be deleted but can be recreated', now(), now());


-- Add garbage_collection fields to existing entities

ALTER TABLE frame ADD COLUMN last_read_date  timestamp without time zone NOT NULL DEFAULT(now());
ALTER TABLE frame ALTER COLUMN name DROP NOT NULL;

ALTER TABLE graph ADD COLUMN last_read_date  timestamp without time zone NOT NULL DEFAULT(now());
ALTER TABLE graph ALTER COLUMN name DROP NOT NULL;

ALTER TABLE model ADD COLUMN last_read_date  timestamp without time zone NOT NULL DEFAULT(now());
ALTER TABLE model ALTER COLUMN name DROP NOT NULL;

