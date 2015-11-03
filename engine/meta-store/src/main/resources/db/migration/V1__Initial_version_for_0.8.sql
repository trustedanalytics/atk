--
-- PostgreSQL Schema Migration
--
-- This file was generated from pg_dump with modifications
-- See FlywayDB.org documentation to see how these changes are applied
--
-- !!! DO NOT MODIFY THIS SCRIPT !!!
--

SET statement_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = off;
SET check_function_bodies = false;
SET client_min_messages = warning;
SET escape_string_warning = off;

SET search_path = public, pg_catalog;

SET default_tablespace = '';

SET default_with_oids = false;

--
-- Name: command; Type: TABLE; Schema: public; Owner: metastore; Tablespace:
--

CREATE TABLE command (
    command_id bigint PRIMARY KEY,
    name character varying(254) NOT NULL,
    arguments text,
    error text,
    complete boolean DEFAULT false NOT NULL,
    result text,
    created_on timestamp without time zone NOT NULL,
    modified_on timestamp without time zone NOT NULL,
    created_by bigint
);


--
-- Name: command_command_id_seq; Type: SEQUENCE; Schema: public; Owner: metastore
--

CREATE SEQUENCE command_command_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MAXVALUE
    NO MINVALUE
    CACHE 1;


--
-- Name: command_command_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: metastore
--

ALTER SEQUENCE command_command_id_seq OWNED BY command.command_id;


--
-- Name: command_command_id_seq; Type: SEQUENCE SET; Schema: public; Owner: metastore
--

SELECT pg_catalog.setval('command_command_id_seq', 1, false);


--
-- Name: frame; Type: TABLE; Schema: public; Owner: metastore; Tablespace:
--

CREATE TABLE frame (
    frame_id bigint PRIMARY KEY,
    name character varying(128) NOT NULL,
    description text,
    uri text NOT NULL,
    schema text NOT NULL,
    status_id bigint DEFAULT 1 NOT NULL,
    created_on timestamp without time zone NOT NULL,
    modified_on timestamp without time zone NOT NULL,
    created_by bigint,
    modified_by bigint
);



--
-- Name: frame_frame_id_seq; Type: SEQUENCE; Schema: public; Owner: metastore
--

CREATE SEQUENCE frame_frame_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MAXVALUE
    NO MINVALUE
    CACHE 1;


--
-- Name: frame_frame_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: metastore
--

ALTER SEQUENCE frame_frame_id_seq OWNED BY frame.frame_id;


--
-- Name: frame_frame_id_seq; Type: SEQUENCE SET; Schema: public; Owner: metastore
--

SELECT pg_catalog.setval('frame_frame_id_seq', 1, false);


--
-- Name: graph; Type: TABLE; Schema: public; Owner: metastore; Tablespace:
--

CREATE TABLE graph (
    graph_id bigint PRIMARY KEY,
    name character varying(128) NOT NULL,
    description text,
    storage character varying(254) NOT NULL,
    status_id bigint DEFAULT 1 NOT NULL,
    created_on timestamp without time zone NOT NULL,
    modified_on timestamp without time zone NOT NULL,
    created_by bigint,
    modified_by bigint
);


--
-- Name: graph_graph_id_seq; Type: SEQUENCE; Schema: public; Owner: metastore
--

CREATE SEQUENCE graph_graph_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MAXVALUE
    NO MINVALUE
    CACHE 1;


--
-- Name: graph_graph_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: metastore
--

ALTER SEQUENCE graph_graph_id_seq OWNED BY graph.graph_id;


--
-- Name: graph_graph_id_seq; Type: SEQUENCE SET; Schema: public; Owner: metastore
--

SELECT pg_catalog.setval('graph_graph_id_seq', 1, false);


--
-- Name: status; Type: TABLE; Schema: public; Owner: metastore; Tablespace:
--

CREATE TABLE status (
    status_id bigint PRIMARY KEY,
    name character varying(128) NOT NULL,
    description text NOT NULL,
    created_on timestamp without time zone NOT NULL,
    modified_on timestamp without time zone NOT NULL
);



--
-- Name: users; Type: TABLE; Schema: public; Owner: metastore; Tablespace:
--

CREATE TABLE users (
    user_id bigint PRIMARY KEY,
    username character varying(254),
    api_key character varying(512),
    created_on timestamp without time zone NOT NULL,
    modified_on timestamp without time zone NOT NULL
);



--
-- Name: users_user_id_seq; Type: SEQUENCE; Schema: public; Owner: metastore
--

CREATE SEQUENCE users_user_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MAXVALUE
    NO MINVALUE
    CACHE 1;



--
-- Name: users_user_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: metastore
--

ALTER SEQUENCE users_user_id_seq OWNED BY users.user_id;


--
-- Name: users_user_id_seq; Type: SEQUENCE SET; Schema: public; Owner: metastore
--

SELECT pg_catalog.setval('users_user_id_seq', 1, false);


--
-- Name: command_id; Type: DEFAULT; Schema: public; Owner: metastore
--

ALTER TABLE ONLY command ALTER COLUMN command_id SET DEFAULT nextval('command_command_id_seq'::regclass);


--
-- Name: frame_id; Type: DEFAULT; Schema: public; Owner: metastore
--

ALTER TABLE ONLY frame ALTER COLUMN frame_id SET DEFAULT nextval('frame_frame_id_seq'::regclass);


--
-- Name: id; Type: DEFAULT; Schema: public; Owner: metastore
--

ALTER TABLE ONLY graph ALTER COLUMN graph_id SET DEFAULT nextval('graph_graph_id_seq'::regclass);


--
-- Name: id; Type: DEFAULT; Schema: public; Owner: metastore
--

ALTER TABLE ONLY users ALTER COLUMN user_id SET DEFAULT nextval('users_user_id_seq'::regclass);


--
-- Data for Name: status; Type: TABLE DATA; Schema: public; Owner: metastore
--

INSERT INTO status (status_id, name, description, created_on, modified_on) VALUES
  (1, 'INIT', 'Initial Status: currently building or initializing', now(), now());
INSERT INTO status (status_id, name, description, created_on, modified_on) VALUES
  (2, 'ACTIVE',	'Active and can be interacted with', now(), now());
INSERT INTO status (status_id, name, description, created_on, modified_on) VALUES
  (3,	'INCOMPLETE',	'Partially created: failure occurred during construction.', now(), now());
INSERT INTO status (status_id, name, description, created_on, modified_on) VALUES
  (4,	'DELETED',	'Deleted but can still be un-deleted, no action has yet been taken on disk', now(), now());
INSERT INTO status (status_id, name, description, created_on, modified_on) VALUES
  (5, 'DELETE_FINAL',	'Underlying storage has been reclaimed, no un-delete is possible', now(), now());


--
-- Name: command_created_by; Type: FK CONSTRAINT; Schema: public; Owner: metastore
--

ALTER TABLE ONLY command
    ADD CONSTRAINT command_created_by FOREIGN KEY (created_by) REFERENCES users(user_id);


--
-- Name: frame_created_by; Type: FK CONSTRAINT; Schema: public; Owner: metastore
--

ALTER TABLE ONLY frame
    ADD CONSTRAINT frame_created_by FOREIGN KEY (created_by) REFERENCES users(user_id);


--
-- Name: frame_modified_by; Type: FK CONSTRAINT; Schema: public; Owner: metastore
--

ALTER TABLE ONLY frame
    ADD CONSTRAINT frame_modified_by FOREIGN KEY (modified_by) REFERENCES users(user_id);


--
-- Name: frame_status_id; Type: FK CONSTRAINT; Schema: public; Owner: metastore
--

ALTER TABLE ONLY frame
    ADD CONSTRAINT frame_status_id FOREIGN KEY (status_id) REFERENCES status(status_id);


--
-- Name: graph_created_by; Type: FK CONSTRAINT; Schema: public; Owner: metastore
--

ALTER TABLE ONLY graph
    ADD CONSTRAINT graph_created_by FOREIGN KEY (created_by) REFERENCES users(user_id);


--
-- Name: graph_modified_by; Type: FK CONSTRAINT; Schema: public; Owner: metastore
--

ALTER TABLE ONLY graph
    ADD CONSTRAINT graph_modified_by FOREIGN KEY (modified_by) REFERENCES users(user_id);


--
-- Name: graph_status_id; Type: FK CONSTRAINT; Schema: public; Owner: metastore
--

ALTER TABLE ONLY graph
    ADD CONSTRAINT graph_status_id FOREIGN KEY (status_id) REFERENCES status(status_id);


