--
-- PostgreSQL database dump
--

-- Dumped from database version 13.7
-- Dumped by pg_dump version 14.4

-- Started on 2023-01-11 10:31:38 MST

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- TOC entry 698 (class 1247 OID 54246)
-- Name: emaildeliverytype; Type: TYPE; Schema: public; Owner: -
--

CREATE DATABASE astro_artifacts WITH TEMPLATE = template0 ENCODING = 'UTF8' LOCALE = 'en_US.UTF8';


\connect astro_artifacts

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;


-- CREATE TYPE public.emaildeliverytype AS ENUM (
--     'attachment',
--     'inline'
-- );


-- --
-- -- TOC entry 707 (class 1247 OID 54390)
-- -- Name: objecttypes; Type: TYPE; Schema: public; Owner: -
-- --

-- CREATE TYPE public.objecttypes AS ENUM (
--     'query',
--     'chart',
--     'dashboard',
--     'dataset'
-- );


-- --
-- -- TOC entry 701 (class 1247 OID 54284)
-- -- Name: sliceemailreportformat; Type: TYPE; Schema: public; Owner: -
-- --

-- CREATE TYPE public.sliceemailreportformat AS ENUM (
--     'visualization',
--     'data'
-- );


-- --
-- -- TOC entry 704 (class 1247 OID 54360)
-- -- Name: tagtypes; Type: TYPE; Schema: public; Owner: -
-- --

-- CREATE TYPE public.tagtypes AS ENUM (
--     'custom',
--     'type',
--     'owner',
--     'favorited_by'
-- );


-- SET default_table_access_method = heap;

--
-- TOC entry 213 (class 1259 OID 52745)
-- Name: alert_query_store; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.alert_query_store (
    edc_alert_query_id bigint NOT NULL,
    search_terms character varying(512) NOT NULL,
    url character varying(255) NOT NULL,
    raw_query_results text,
    date_created timestamp without time zone DEFAULT now()
);


--
-- TOC entry 212 (class 1259 OID 52743)
-- Name: alert_query_store_edc_alert_query_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.alert_query_store_edc_alert_query_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- TOC entry 3704 (class 0 OID 0)
-- Dependencies: 212
-- Name: alert_query_store_edc_alert_query_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.alert_query_store_edc_alert_query_id_seq OWNED BY public.alert_query_store.edc_alert_query_id;


--
-- TOC entry 211 (class 1259 OID 52597)
-- Name: alert_stream_payloads; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.alert_stream_payloads (
    edc_alert_stream_id bigint NOT NULL,
    topic character(255) NOT NULL,
    url character(500) NOT NULL,
    raw_payload text NOT NULL,
    date_received timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    science_stamp_url character varying(255),
    difference_stamp_url character varying(255),
    template_stamp_url character varying(255)
);


--
-- TOC entry 210 (class 1259 OID 52595)
-- Name: alert_stream_payloads_edc_alert_stream_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.alert_stream_payloads_edc_alert_stream_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- TOC entry 3705 (class 0 OID 0)
-- Dependencies: 210
-- Name: alert_stream_payloads_edc_alert_stream_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.alert_stream_payloads_edc_alert_stream_id_seq OWNED BY public.alert_stream_payloads.edc_alert_stream_id;


--
-- TOC entry 200 (class 1259 OID 20741)
-- Name: astro_objects; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.astro_objects (
    id bigint NOT NULL,
    "sourceId" bigint,
    "objectId" bigint,
    ra numeric,
    "dec" numeric,
    type character varying(20),
    brightness numeric,
    distance numeric
);


--
-- TOC entry 208 (class 1259 OID 36368)
-- Name: citizen_science_batches; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.citizen_science_batches (
    cit_sci_batch_id bigint NOT NULL,
    cit_sci_proj_id bigint NOT NULL,
    vendor_batch_id bigint NOT NULL,
    batch_status character varying(30) NOT NULL,
    date_created timestamp with time zone DEFAULT now() NOT NULL,
    date_last_updated timestamp with time zone,
    manifest_url character varying(255)
);


--
-- TOC entry 209 (class 1259 OID 36394)
-- Name: citizen_science_batches_cit_sci_batch_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

ALTER TABLE public.citizen_science_batches ALTER COLUMN cit_sci_batch_id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.citizen_science_batches_cit_sci_batch_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- TOC entry 201 (class 1259 OID 20799)
-- Name: citizen_science_meta; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.citizen_science_meta (
    cit_sci_meta_id bigint NOT NULL,
    edc_ver_id bigint,
    source_id bigint,
    source_id_type character varying(30),
    uri character varying(255),
    date_created timestamp with time zone DEFAULT now(),
    public boolean NOT NULL,
    user_defined_values character varying(500)
);


--
-- TOC entry 202 (class 1259 OID 27733)
-- Name: citizen_science_meta_cit_sci_meta_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

ALTER TABLE public.citizen_science_meta ALTER COLUMN cit_sci_meta_id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.citizen_science_meta_cit_sci_meta_id_seq
    START WITH 4
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- TOC entry 203 (class 1259 OID 27782)
-- Name: citizen_science_owners; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.citizen_science_owners (
    cit_sci_owner_id bigint NOT NULL,
    email character varying(50) NOT NULL,
    status character varying(30) NOT NULL,
    date_created timestamp with time zone DEFAULT now() NOT NULL
);


--
-- TOC entry 204 (class 1259 OID 27787)
-- Name: citizen_science_owners_cit_sci_owner_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

ALTER TABLE public.citizen_science_owners ALTER COLUMN cit_sci_owner_id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.citizen_science_owners_cit_sci_owner_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- TOC entry 207 (class 1259 OID 27806)
-- Name: citizen_science_proj_meta_lookup; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.citizen_science_proj_meta_lookup (
    cit_sci_proj_id bigint NOT NULL,
    cit_sci_meta_id bigint NOT NULL,
    cit_sci_batch_id bigint,
    cit_sci_lookup_id bigint NOT NULL
);


--
-- TOC entry 214 (class 1259 OID 53145)
-- Name: citizen_science_proj_meta_lookup_cit_sci_lookup_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

ALTER TABLE public.citizen_science_proj_meta_lookup ALTER COLUMN cit_sci_lookup_id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.citizen_science_proj_meta_lookup_cit_sci_lookup_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- TOC entry 205 (class 1259 OID 27791)
-- Name: citizen_science_projects; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.citizen_science_projects (
    cit_sci_proj_id bigint NOT NULL,
    vendor_project_id bigint NOT NULL,
    owner_id bigint NOT NULL,
    project_status character varying(30) NOT NULL,
    excess_data_exception boolean DEFAULT false NOT NULL,
    date_created timestamp with time zone DEFAULT now() NOT NULL,
    date_completed timestamp with time zone,
    data_rights_approved boolean DEFAULT false NOT NULL
);


--
-- TOC entry 206 (class 1259 OID 27796)
-- Name: citizen_science_projects_cit_sci_proj_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

ALTER TABLE public.citizen_science_projects ALTER COLUMN cit_sci_proj_id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.citizen_science_projects_cit_sci_proj_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- TOC entry 218 (class 1259 OID 53261)
-- Name: data_release_diaobjects; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.data_release_diaobjects (
    edc_diaobj_ver_id bigint NOT NULL,
    decl double precision,
    ra double precision,
    gpsfluxchi2 double precision,
    ipsfluxchi2 double precision,
    rpsfluxchi2 double precision,
    upsfluxchi2 double precision,
    ypsfluxchi2 double precision,
    zpsfluxchi2 double precision,
    gpsfluxmax double precision,
    ipsfluxmax double precision,
    rpsfluxmax double precision,
    upsfluxmax double precision,
    ypsfluxmax double precision,
    zpsfluxmax double precision,
    gpsfluxmin double precision,
    ipsfluxmin double precision,
    rpsfluxmin double precision,
    upsfluxmin double precision,
    ypsfluxmin double precision,
    zpsfluxmin double precision,
    gpsfluxmean double precision,
    ipsfluxmean double precision,
    rpsfluxmean double precision,
    upsfluxmean double precision,
    ypsfluxmean double precision,
    zpsfluxmean double precision,
    gpsfluxndata double precision,
    ipsfluxndata double precision,
    rpsfluxndata double precision,
    upsfluxndata double precision,
    ypsfluxndata double precision,
    zpsfluxndata double precision
);


--
-- TOC entry 217 (class 1259 OID 53259)
-- Name: data_release_diaobjects_edc_diaobj_ver_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

ALTER TABLE public.data_release_diaobjects ALTER COLUMN edc_diaobj_ver_id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.data_release_diaobjects_edc_diaobj_ver_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- TOC entry 220 (class 1259 OID 53267)
-- Name: data_release_forcedsources; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.data_release_forcedsources (
    edc_forcedsource_ver_id bigint NOT NULL,
    forcedsourceid bigint,
    objectid bigint,
    parentobjectid bigint,
    coord_ra double precision,
    coord_dec double precision,
    skymap character varying(255),
    tract bigint,
    patch bigint,
    band character varying(255),
    ccdvisitid bigint,
    detect_ispatchinner boolean,
    detect_isprimary boolean,
    detect_istractinner boolean,
    localbackground_instfluxerr double precision,
    localbackground_instflux double precision,
    localphotocaliberr double precision,
    localphotocalib_flag boolean,
    localphotocalib double precision,
    localwcs_cdmatrix_1_1 double precision,
    localwcs_cdmatrix_1_2 double precision,
    localwcs_cdmatrix_2_1 double precision,
    localwcs_cdmatrix_2_2 double precision,
    localwcs_flag boolean,
    pixelflags_bad boolean,
    pixelflags_crcenter boolean,
    pixelflags_cr boolean,
    pixelflags_edge boolean,
    pixelflags_interpolatedcenter boolean,
    pixelflags_interpolated boolean,
    pixelflags_saturatedcenter boolean,
    pixelflags_saturated boolean,
    pixelflags_suspectcenter boolean,
    pixelflags_suspect boolean,
    psfdifffluxerr double precision,
    psfdiffflux_flag boolean,
    psfdiffflux double precision,
    psffluxerr double precision,
    psfflux_flag boolean,
    psfflux double precision
);


--
-- TOC entry 219 (class 1259 OID 53265)
-- Name: data_release_forcedsource_edc_forcedsource_ver_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

ALTER TABLE public.data_release_forcedsources ALTER COLUMN edc_forcedsource_ver_id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.data_release_forcedsource_edc_forcedsource_ver_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- TOC entry 216 (class 1259 OID 53256)
-- Name: data_release_objects; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.data_release_objects (
    edc_obj_ver_id bigint NOT NULL,
    objectid bigint NOT NULL,
    coord_dec double precision,
    coord_ra double precision,
    g_ra double precision,
    i_ra double precision,
    r_ra double precision,
    u_ra double precision,
    y_ra double precision,
    z_ra double precision,
    g_decl double precision,
    i_decl double precision,
    r_decl double precision,
    u_decl double precision,
    y_decl double precision,
    z_decl double precision,
    "g_bdFluxB" double precision,
    "i_bdFluxB" double precision,
    "r_bdFluxB" double precision,
    "u_bdFluxB" double precision,
    "y_bdFluxB" double precision,
    "z_bdFluxB" double precision,
    "g_bdFluxD" double precision,
    "i_bdFluxD" double precision,
    "r_bdFluxD" double precision,
    "u_bdFluxD" double precision,
    "y_bdFluxD" double precision,
    "z_bdFluxD" double precision,
    "g_bdReB" double precision,
    "i_bdReB" double precision,
    "r_bdReB" double precision,
    "u_bdReB" double precision,
    "y_bdReB" double precision,
    "z_bdReB" double precision,
    "g_bdReD" double precision,
    "i_bdReD" double precision,
    "r_bdReD" double precision,
    "u_bdReD" double precision,
    "y_bdReD" double precision,
    "z_bdReD" double precision
);


--
-- TOC entry 215 (class 1259 OID 53254)
-- Name: data_release_objects_edc_obj_ver_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

ALTER TABLE public.data_release_objects ALTER COLUMN edc_obj_ver_id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.data_release_objects_edc_obj_ver_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- TOC entry 222 (class 1259 OID 55177)
-- Name: edc_logger_ai_pk; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.edc_logger_ai_pk
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- TOC entry 221 (class 1259 OID 55167)
-- Name: edc_logger; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.edc_logger (
    edc_logger_id bigint DEFAULT nextval('public.edc_logger_ai_pk'::regclass) NOT NULL,
    application_name character varying(255) NOT NULL,
    run_id character varying(100) NOT NULL,
    notes character varying(500) NOT NULL,
    category character varying(100) NOT NULL,
    environment character varying(50) NOT NULL,
    date_created timestamp with time zone DEFAULT now() NOT NULL
);


--
-- TOC entry 3539 (class 2604 OID 52748)
-- Name: alert_query_store edc_alert_query_id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.alert_query_store ALTER COLUMN edc_alert_query_id SET DEFAULT nextval('public.alert_query_store_edc_alert_query_id_seq'::regclass);


--
-- TOC entry 3537 (class 2604 OID 52600)
-- Name: alert_stream_payloads edc_alert_stream_id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.alert_stream_payloads ALTER COLUMN edc_alert_stream_id SET DEFAULT nextval('public.alert_stream_payloads_edc_alert_stream_id_seq'::regclass);


--
-- TOC entry 3562 (class 2606 OID 52753)
-- Name: alert_query_store alert_query_store_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.alert_query_store
    ADD CONSTRAINT alert_query_store_pkey PRIMARY KEY (edc_alert_query_id);


--
-- TOC entry 3560 (class 2606 OID 52605)
-- Name: alert_stream_payloads alert_stream_payloads_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.alert_stream_payloads
    ADD CONSTRAINT alert_stream_payloads_pkey PRIMARY KEY (edc_alert_stream_id);


--
-- TOC entry 3544 (class 2606 OID 20748)
-- Name: astro_objects astro_objects_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.astro_objects
    ADD CONSTRAINT astro_objects_pkey PRIMARY KEY (id);


--
-- TOC entry 3546 (class 2606 OID 20803)
-- Name: citizen_science_meta citizen_science_meta_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.citizen_science_meta
    ADD CONSTRAINT citizen_science_meta_pkey PRIMARY KEY (cit_sci_meta_id);


--
-- TOC entry 3548 (class 2606 OID 27856)
-- Name: citizen_science_owners citizen_science_owners_email_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.citizen_science_owners
    ADD CONSTRAINT citizen_science_owners_email_key UNIQUE (email);


--
-- TOC entry 3550 (class 2606 OID 27786)
-- Name: citizen_science_owners citizen_science_owners_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.citizen_science_owners
    ADD CONSTRAINT citizen_science_owners_pkey PRIMARY KEY (cit_sci_owner_id);


--
-- TOC entry 3555 (class 2606 OID 53144)
-- Name: citizen_science_proj_meta_lookup citizen_science_proj_meta_lookup_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.citizen_science_proj_meta_lookup
    ADD CONSTRAINT citizen_science_proj_meta_lookup_pkey PRIMARY KEY (cit_sci_lookup_id);


--
-- TOC entry 3552 (class 2606 OID 27795)
-- Name: citizen_science_projects citizen_science_projects_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.citizen_science_projects
    ADD CONSTRAINT citizen_science_projects_pkey PRIMARY KEY (cit_sci_proj_id);


--
-- TOC entry 3564 (class 2606 OID 55175)
-- Name: edc_logger edc_logger_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.edc_logger
    ADD CONSTRAINT edc_logger_pkey PRIMARY KEY (edc_logger_id);


--
-- TOC entry 3558 (class 2606 OID 36375)
-- Name: citizen_science_batches uniqueness2; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.citizen_science_batches
    ADD CONSTRAINT uniqueness2 UNIQUE (cit_sci_batch_id);


--
-- TOC entry 3553 (class 1259 OID 27820)
-- Name: citizen_science_proj_meta_lookup_meta_id_fk; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX citizen_science_proj_meta_lookup_meta_id_fk ON public.citizen_science_proj_meta_lookup USING btree (cit_sci_meta_id);


--
-- TOC entry 3556 (class 1259 OID 27814)
-- Name: citizen_science_proj_meta_project_id_fk; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX citizen_science_proj_meta_project_id_fk ON public.citizen_science_proj_meta_lookup USING btree (cit_sci_proj_id);


--
-- TOC entry 3568 (class 2606 OID 36381)
-- Name: citizen_science_proj_meta_lookup citizen_science_proj_meta_lookup_cit_sci_batch_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.citizen_science_proj_meta_lookup
    ADD CONSTRAINT citizen_science_proj_meta_lookup_cit_sci_batch_id_fkey FOREIGN KEY (cit_sci_batch_id) REFERENCES public.citizen_science_batches(cit_sci_batch_id);


--
-- TOC entry 3567 (class 2606 OID 27815)
-- Name: citizen_science_proj_meta_lookup citizen_science_proj_meta_lookup_cit_sci_meta_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.citizen_science_proj_meta_lookup
    ADD CONSTRAINT citizen_science_proj_meta_lookup_cit_sci_meta_id_fkey FOREIGN KEY (cit_sci_meta_id) REFERENCES public.citizen_science_meta(cit_sci_meta_id) NOT VALID;


--
-- TOC entry 3566 (class 2606 OID 27809)
-- Name: citizen_science_proj_meta_lookup citizen_science_proj_meta_lookup_cit_sci_proj_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.citizen_science_proj_meta_lookup
    ADD CONSTRAINT citizen_science_proj_meta_lookup_cit_sci_proj_id_fkey FOREIGN KEY (cit_sci_proj_id) REFERENCES public.citizen_science_projects(cit_sci_proj_id) NOT VALID;


--
-- TOC entry 3565 (class 2606 OID 27799)
-- Name: citizen_science_projects citizen_science_projects_owner_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.citizen_science_projects
    ADD CONSTRAINT citizen_science_projects_owner_id_fkey FOREIGN KEY (owner_id) REFERENCES public.citizen_science_owners(cit_sci_owner_id) NOT VALID;


-- Completed on 2023-01-11 10:32:02 MST

--
-- PostgreSQL database dump complete
--

