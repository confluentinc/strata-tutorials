-- data dictionary at http://www.nyc.gov/html/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf
create table yellow_cab_trips (
    VendorID NUMERIC(1),
    tpep_pickup_datetime DATETIME,
    tpep_dropoff_datetime DATETIME,
    passenger_count NUMERIC(1),
    trip_distance FLOAT,
    pickup_longitude NUMERIC,
    pickup_latitude NUMERIC,
    RatecodeID NUMERIC(1),
    store_and_fwd_flag CHAR(1),
    dropoff_longitude NUMERIC,
    dropoff_latitude NUMERIC,
    payment_type NUMERIC(1),
    fare_amount NUMERIC,
    extra NUMERIC,
    mta_tax NUMERIC,
    tip_amount NUMERIC,
    tolls_amount NUMERIC,
    improvement_surcharge NUMERIC,
    total_amount NUMERIC
);
.mode csv
.import yellow_tripdata_2015-12.nohdr.csv yellow_cab_trips

create table weather_raw (
      USAF NUMERIC(6),
      WBAN NUMERIC(5),
      TS DATETIME,
      DIR VARCHAR(3),
      SPD VARCHAR(3),
      GUS VARCHAR(3),
      CLG VARCHAR(3),
      SKC VARCHAR(3),
      VSB VARCHAR(2.1),
      TEMP VARCHAR(4),
      DEWP VARCHAR(4),
      SLP VARCHAR(4.1),
      ALT VARCHAR(2.2),
      STP VARCHAR(4.1),
      MAXT VARCHAR(4),
      MINT VARCHAR(4),
      PCP01 VARCHAR(4),
      PCP06 VARCHAR(4),
      PCP24 VARCHAR(4),
      PCPXX VARCHAR(4),
      SD VARCHAR(2)
      );

.mode csv
.import data/central_park_weather.csv weather_raw

create table weather AS SELECT
    USAF, WBAN, TS,
    CAST(NULLIF(DIR,'')   AS NUMERIC) DIR,
    CAST(NULLIF(SPD  ,'') AS NUMERIC) SPD,
    CAST(NULLIF(GUS,'')   AS NUMERIC) GUS,
    CAST(NULLIF(CLG,'')   AS NUMERIC) CLG,
    NULLIF(SKC,'') SKC,
    CAST(NULLIF(VSB,'')  AS NUMERIC) VSB,
    CAST(NULLIF(TEMP,'') AS NUMERIC) TEMP,
    CAST(NULLIF(DEWP,'') AS NUMERIC) DEWP,
    CAST(NULLIF(SLP,'')  AS NUMERIC) SLP,
    CAST(NULLIF(ALT,'')  AS NUMERIC) ALT,
    CAST(NULLIF(STP,'')  AS NUMERIC) STP,
    CAST(NULLIF(MAXT,'') AS NUMERIC) MAXT,
    CAST(NULLIF(MINT,'') AS NUMERIC) MINT,
    NULLIF(PCP01,'') PCP01,
    NULLIF(PCP06,'') PCP06,
    NULLIF(PCP24,'') PCP24,
    NULLIF(PCPXX,'') PCPXX,
    NULLIF(SD,'') SD
FROM weather_raw;
drop table weather_raw;
commit;



