--
-- PostgreSQL Schema Migration
--
-- This file was generated from pg_dump with modifications
-- See FlywayDB.org documentation to see how these changes are applied
--
-- !!! DO NOT MODIFY THIS SCRIPT !!!
--

--
-- Name: jobcontext; Type: TABLE; Schema: public; Owner: metastore; Tablespace:
--
CREATE TABLE jobcontext (
  jobcontext_id bigint PRIMARY KEY,
  user_id bigint REFERENCES users (user_id),
  yarn_app_name character varying(254) UNIQUE,
  yarn_app_id character varying(254) UNIQUE,
  client_id character varying(254) NOT NULL,
  created_on timestamp without time zone NOT NULL,
  modified_on timestamp without time zone NOT NULL,
  progress text,
  job_server_uri character varying(254)
);

--
-- Name: jobcontext_id_seq; Type: SEQUENCE; Schema: public; Owner: metastore
--
CREATE SEQUENCE jobcontext_id_seq
  START WITH 1
  INCREMENT BY 1
  NO MAXVALUE
  NO MINVALUE
  CACHE 1;

--
-- Name: jobcontext_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: metastore
--

ALTER SEQUENCE jobcontext_id_seq OWNED BY jobcontext.jobcontext_id;


--
-- Name: jobcontext_id_seq; Type: SEQUENCE SET; Schema: public; Owner: metastore
--

SELECT pg_catalog.setval('jobcontext_id_seq', 1, false);

--
-- Name: jobcontext_id; Type: DEFAULT; Schema: public; Owner: metastore
--

ALTER TABLE ONLY jobcontext ALTER COLUMN jobcontext_id SET DEFAULT nextval('jobcontext_id_seq'::regclass);
