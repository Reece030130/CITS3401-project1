BEGIN;


CREATE TABLE IF NOT EXISTS public.dim_crash
(
    crashid character varying(50) COLLATE pg_catalog."default" NOT NULL,
    crashtype character varying(50) COLLATE pg_catalog."default",
    CONSTRAINT dim_crash_pkey PRIMARY KEY (crashid)
);

CREATE TABLE IF NOT EXISTS public.dim_date
(
    dateid character varying(50) COLLATE pg_catalog."default" NOT NULL,
    dayofweek character varying(20) COLLATE pg_catalog."default",
    dayweek character varying(20) COLLATE pg_catalog."default",
    month character varying(20) COLLATE pg_catalog."default",
    year character varying(20) COLLATE pg_catalog."default",
    CONSTRAINT dim_date_pkey PRIMARY KEY (dateid)
);

CREATE TABLE IF NOT EXISTS public.dim_fatalitiespersonalinfo
(
    fatalitypersonalinfoid character varying(50) COLLATE pg_catalog."default" NOT NULL,
    agegroup character varying(20) COLLATE pg_catalog."default",
    age character varying(20) COLLATE pg_catalog."default",
    gender character varying(10) COLLATE pg_catalog."default",
    CONSTRAINT dim_fatalitiespersonalinfo_pkey PRIMARY KEY (fatalitypersonalinfoid)
);

CREATE TABLE IF NOT EXISTS public.dim_geography
(
    geoid character varying(50) COLLATE pg_catalog."default" NOT NULL,
    stename21 character varying(50) COLLATE pg_catalog."default",
    sa4name21 character varying(100) COLLATE pg_catalog."default",
    lganame21 character varying(50) COLLATE pg_catalog."default",
    nationalremotenessareas character varying(50) COLLATE pg_catalog."default",
    CONSTRAINT dim_geography_pkey PRIMARY KEY (geoid)
);

CREATE TABLE IF NOT EXISTS public.dim_holiday
(
    holidayid character varying(50) COLLATE pg_catalog."default" NOT NULL,
    christmasperiod character varying(50) COLLATE pg_catalog."default",
    easterperiod character varying(50) COLLATE pg_catalog."default",
    CONSTRAINT dim_holiday_pkey PRIMARY KEY (holidayid)
);

CREATE TABLE IF NOT EXISTS public.dim_road
(
    roadid character varying(50) COLLATE pg_catalog."default" NOT NULL,
    nationalroadtype character varying(50) COLLATE pg_catalog."default",
    roaduser character varying(50) COLLATE pg_catalog."default",
    CONSTRAINT dim_road_pkey PRIMARY KEY (roadid)
);

CREATE TABLE IF NOT EXISTS public.dim_time
(
    timeid character varying(50) COLLATE pg_catalog."default" NOT NULL,
    timeofday character varying(20) COLLATE pg_catalog."default",
    time TIME,
    CONSTRAINT dim_time_pkey PRIMARY KEY (timeid)
);

CREATE TABLE IF NOT EXISTS public.dim_vehicle
(
    vehicleid character varying(50) COLLATE pg_catalog."default" NOT NULL,
    businvolvement character varying(50) COLLATE pg_catalog."default",
    heavyrigidtruckinvolvement character varying(50) COLLATE pg_catalog."default",
    articulatedtruckinvolvement character varying(50) COLLATE pg_catalog."default",
    CONSTRAINT dim_vehicle_pkey PRIMARY KEY (vehicleid)
);

CREATE TABLE IF NOT EXISTS public.fact_table
(
    fatalityid character varying(50) COLLATE pg_catalog."default" NOT NULL,
    crash_id character varying(50) COLLATE pg_catalog."default",
    speedlimit character varying(20) COLLATE pg_catalog."default",
    numberfatalities character varying(20) COLLATE pg_catalog."default",
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


COPY dim_crash FROM '/tmp/Project1/dim_crash.csv' WITH (FORMAT csv, HEADER true);

-- dim_date
COPY dim_date FROM '/tmp/Project1/dim_date.csv' WITH (FORMAT csv, HEADER true);

-- dim_fatalitiesPersonalInfo
COPY dim_fatalitiesPersonalInfo FROM '/tmp/Project1/dim_fatalitiesPersonalInfo.csv' WITH (FORMAT csv, HEADER true);

-- dim_geography
COPY dim_geography FROM '/tmp/Project1/dim_geography.csv' WITH (FORMAT csv, HEADER true);

-- dim_holiday
COPY dim_holiday FROM '/tmp/Project1/dim_holiday.csv' WITH (FORMAT csv, HEADER true);

-- dim_road
COPY dim_road FROM '/tmp/Project1/dim_road_df.csv' WITH (FORMAT csv, HEADER true);

-- dim_time
COPY dim_time FROM '/tmp/Project1/dim_time_df.csv' WITH (FORMAT csv, HEADER true);

-- dim_vehicle
COPY dim_vehicle FROM '/tmp/Project1/dim_vehicle_df.csv' WITH (FORMAT csv, HEADER true);

-- fact_table
COPY fact_table FROM '/tmp/Project1/fact_table.csv' WITH (FORMAT csv, HEADER true);



select * from dim_holiday
