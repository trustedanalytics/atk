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
-- Name: model; Type: TABLE; Schema: public; Owner: metastore; Tablespace:
--
CREATE TABLE model (
  model_id bigint PRIMARY KEY,
  name character varying(254) NOT NULL,
  model_type character varying(254) NOT NULL,
  description text,
  status_id bigint,
  data text,
  created_on timestamp without time zone NOT NULL,
  modified_on timestamp without time zone NOT NULL,
  created_by bigint,
  modified_by bigint
);

--
-- Name: model_model_id_seq; Type: SEQUENCE; Schema: public; Owner: metastore
--
CREATE SEQUENCE model_model_id_seq
  START WITH 1
  INCREMENT BY 1
  NO MAXVALUE
  NO MINVALUE
  CACHE 1;

--
-- Name: model_model_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: metastore
--

ALTER SEQUENCE model_model_id_seq OWNED BY model.model_id;


--
-- Name: model_model_id_seq; Type: SEQUENCE SET; Schema: public; Owner: metastore
--

SELECT pg_catalog.setval('model_model_id_seq', 1, false);

--
-- Name: model_id; Type: DEFAULT; Schema: public; Owner: metastore
--

ALTER TABLE ONLY model ALTER COLUMN model_id SET DEFAULT nextval('model_model_id_seq'::regclass);
