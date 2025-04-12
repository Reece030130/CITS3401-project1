-- ========================================================
-- TABLE: public.dim_crash
-- ========================================================
CREATE TABLE IF NOT EXISTS public.dim_crash
(
    crashid character varying(50) COLLATE pg_catalog."default" NOT NULL,
    crashtype character varying(50) COLLATE pg_catalog."default",
    CONSTRAINT dim_crash_pkey PRIMARY KEY (crashid)
);

-- Reminder: Change the file path below to your actual file location if needed.
COPY dim_crash FROM 'path/to/dim_crash.csv' WITH (FORMAT csv, HEADER true);


-- ========================================================
-- TABLE: public.dim_date
-- ========================================================
CREATE TABLE IF NOT EXISTS public.dim_date
(
    dateid character varying(50) COLLATE pg_catalog."default" NOT NULL,
    dayofweek character varying(20) COLLATE pg_catalog."default",
    dayweek character varying(20) COLLATE pg_catalog."default",
    month INTEGER,
    year INTEGER,
    CONSTRAINT dim_date_pkey PRIMARY KEY (dateid)
);

-- Reminder: Update the file location below from '/tmp/project1/dim_date.csv' to your desired path.
COPY dim_date FROM 'path/to/dim_date.csv' WITH (FORMAT csv, HEADER true);


-- ========================================================
-- TABLE: public.dim_fatalitiespersonalinfo
-- ========================================================
CREATE TABLE IF NOT EXISTS public.dim_fatalitiespersonalinfo
(
    fatalitypersonalinfoid character varying(50) COLLATE pg_catalog."default" NOT NULL,
    agegroup character varying(20) COLLATE pg_catalog."default",
    age INTEGER,
    gender character varying(10) COLLATE pg_catalog."default",
    CONSTRAINT dim_fatalitiespersonalinfo_pkey PRIMARY KEY (fatalitypersonalinfoid)
);

-- Reminder: Change the file path below accordingly.
COPY dim_fatalitiesPersonalInfo FROM 'path/to/dim_fatalitiesPersonalInfo.csv' WITH (FORMAT csv, HEADER true);


-- ========================================================
-- TABLE: public.dim_geography
-- ========================================================
CREATE TABLE IF NOT EXISTS public.dim_geography
(
    geoid character varying(50) COLLATE pg_catalog."default" NOT NULL,
    stename21 character varying(50) COLLATE pg_catalog."default",
    sa4name21 character varying(100) COLLATE pg_catalog."default",
    lganame21 character varying(50) COLLATE pg_catalog."default",
    nationalremotenessareas character varying(50) COLLATE pg_catalog."default",
    CONSTRAINT dim_geography_pkey PRIMARY KEY (geoid)
);

-- Reminder: Update the file location from '/tmp/project1/dim_geography.csv' if needed.
COPY dim_geography FROM 'path/to/dim_geography.csv' WITH (FORMAT csv, HEADER true);


-- ========================================================
-- TABLE: public.dim_holiday
-- ========================================================
CREATE TABLE IF NOT EXISTS public.dim_holiday
(
    holidayid character varying(50) COLLATE pg_catalog."default" NOT NULL,
    christmasperiod character varying(50) COLLATE pg_catalog."default",
    easterperiod character varying(50) COLLATE pg_catalog."default",
    CONSTRAINT dim_holiday_pkey PRIMARY KEY (holidayid)
);

-- Reminder: Modify the file path to your actual folder location.
COPY dim_holiday FROM 'path/to/dim_holiday.csv' WITH (FORMAT csv, HEADER true);


-- ========================================================
-- TABLE: public.dim_road
-- ========================================================
CREATE TABLE IF NOT EXISTS public.dim_road
(
    roadid character varying(50) COLLATE pg_catalog."default" NOT NULL,
    nationalroadtype character varying(50) COLLATE pg_catalog."default",
    roaduser character varying(50) COLLATE pg_catalog."default",
    CONSTRAINT dim_road_pkey PRIMARY KEY (roadid)
);

-- Reminder: Ensure the file path is updated accordingly.
COPY dim_road FROM 'path/to/dim_road_df.csv' WITH (FORMAT csv, HEADER true);


-- ========================================================
-- TABLE: public.dim_time
-- ========================================================
CREATE TABLE IF NOT EXISTS public.dim_time
(
    timeid character varying(50) COLLATE pg_catalog."default" NOT NULL,
    timeofday character varying(20) COLLATE pg_catalog."default",
    time TIME,
    CONSTRAINT dim_time_pkey PRIMARY KEY (timeid)
);

-- Reminder: Update the file path below to match your environment.
COPY dim_time FROM 'path/to/dim_time.csv' WITH (FORMAT csv, HEADER true);


-- ========================================================
-- TABLE: public.dim_vehicle
-- ========================================================
CREATE TABLE IF NOT EXISTS public.dim_vehicle
(
    vehicleid character varying(50) COLLATE pg_catalog."default" NOT NULL,
    businvolvement character varying(50) COLLATE pg_catalog."default",
    heavyrigidtruckinvolvement character varying(50) COLLATE pg_catalog."default",
    articulatedtruckinvolvement character varying(50) COLLATE pg_catalog."default",
    CONSTRAINT dim_vehicle_pkey PRIMARY KEY (vehicleid)
);

-- Reminder: Change '/tmp/project1/dim_vehicle_df.csv' to your actual file path.
COPY dim_vehicle FROM 'path/to/dim_vehicle_df.csv' WITH (FORMAT csv, HEADER true);


-- ========================================================
-- TABLE: public.fact_table
-- ========================================================
CREATE TABLE IF NOT EXISTS public.fact_table
(
    fatalityid character varying(50) COLLATE pg_catalog."default" NOT NULL,
    crash_id character varying(50) COLLATE pg_catalog."default",
    speedlimit INTEGER,
    numberfatalities INTEGER,
    is_holiday BOOLEAN,
    avg_age NUMERIC,
    dateid character varying(50) COLLATE pg_catalog."default",
    geoid character varying(50) COLLATE pg_catalog."default",
    fatalitypersonalinfoid character varying(50) COLLATE pg_catalog."default",
    crashid character varying(50) COLLATE pg_catalog."default",
    roadid character varying(50) COLLATE pg_catalog."default",
    vehicleid character varying(50) COLLATE pg_catalog."default",
    timeid character varying(50) COLLATE pg_catalog."default",
    holidayid character varying(50) COLLATE pg_catalog."default",
    CONSTRAINT fact_table_pkey PRIMARY KEY (fatalityid)
);

-- Reminder: Update the COPY file path from '/tmp/project1/fact_table.csv' to match your actual data location.
COPY fact_table FROM 'path/to/fact_table.csv' WITH (FORMAT csv, HEADER true);


-- ========================================================
-- ADD FOREIGN KEY CONSTRAINTS TO FACT_TABLE
-- ========================================================

ALTER TABLE IF EXISTS public.fact_table
    ADD CONSTRAINT fk_crashid FOREIGN KEY (crashid)
    REFERENCES public.dim_crash (crashid) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION;

ALTER TABLE IF EXISTS public.fact_table
    ADD CONSTRAINT fk_dateid FOREIGN KEY (dateid)
    REFERENCES public.dim_date (dateid) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION;

ALTER TABLE IF EXISTS public.fact_table
    ADD CONSTRAINT fk_fatalitypersonalinfoid FOREIGN KEY (fatalitypersonalinfoid)
    REFERENCES public.dim_fatalitiespersonalinfo (fatalitypersonalinfoid) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION;

ALTER TABLE IF EXISTS public.fact_table
    ADD CONSTRAINT fk_geoid FOREIGN KEY (geoid)
    REFERENCES public.dim_geography (geoid) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION;

ALTER TABLE IF EXISTS public.fact_table
    ADD CONSTRAINT fk_holidayid FOREIGN KEY (holidayid)
    REFERENCES public.dim_holiday (holidayid) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION;

ALTER TABLE IF EXISTS public.fact_table
    ADD CONSTRAINT fk_roadid FOREIGN KEY (roadid)
    REFERENCES public.dim_road (roadid) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION;

ALTER TABLE IF EXISTS public.fact_table
    ADD CONSTRAINT fk_timeid FOREIGN KEY (timeid)
    REFERENCES public.dim_time (timeid) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION;

ALTER TABLE IF EXISTS public.fact_table
    ADD CONSTRAINT fk_vehicleid FOREIGN KEY (vehicleid)
    REFERENCES public.dim_vehicle (vehicleid) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION;
