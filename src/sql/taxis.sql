-- data dictionary at http://www.nyc.gov/html/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf
create table yellow_cab_trips_raw (
    VendorID NUMERIC(1),
    tpep_pickup_datetime DATETIME NOT NULL,
    tpep_dropoff_datetime DATETIME,
    passenger_count TINYINT,
    trip_distance FLOAT,
    pickup_longitude DOUBLE,
    pickup_latitude DOUBLE,
    RatecodeID TINYINT,
    store_and_fwd_flag CHAR(1),
    dropoff_longitude DOUBLE,
    dropoff_latitude DOUBLE,
    payment_type TINYINT,
    fare_amount FLOAT,
    extra FLOAT,
    mta_tax FLOAT,
    tip_amount FLOAT,
    tolls_amount FLOAT,
    improvement_surcharge FLOAT,
    total_amount FLOAT
);
.mode csv
.import yellow_tripdata_2015-12.nohdr.csv yellow_cab_trips_raw

create table yellow_cab_trips (
    VendorID NUMERIC(1),
    tpep_pickup_datetime INTEGER NOT NULL,
    tpep_dropoff_datetime INTEGER,
    passenger_count TINYINT,
    trip_distance FLOAT,
    pickup_longitude DOUBLE,
    pickup_latitude DOUBLE,
    RatecodeID TINYINT,
    store_and_fwd_flag CHAR(1),
    dropoff_longitude DOUBLE,
    dropoff_latitude DOUBLE,
    payment_type TINYINT,
    fare_amount FLOAT,
    extra FLOAT,
    mta_tax FLOAT,
    tip_amount FLOAT,
    tolls_amount FLOAT,
    improvement_surcharge FLOAT,
    total_amount FLOAT,
    uid INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL
);

insert into yellow_cab_trips
 select VendorID,
    CAST(strftime("%s",tpep_pickup_datetime) as INTEGER) tpep_pickup_datetime,
    CAST(strftime("%s",tpep_dropoff_datetime) as INTEGER) tpep_dropoff_datetime,
    passenger_count,
    trip_distance,
    pickup_longitude,
    pickup_latitude,
    RatecodeID,
    store_and_fwd_flag,
    dropoff_longitude,
    dropoff_latitude,
    payment_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    ROWID
  from yellow_cab_trips_raw;

drop table yellow_cab_trips_raw;


create table weather_raw (
      USAF NUMERIC(6),
      WBAN NUMERIC(5),
      TS DATETIME NOT NULL,
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
.import central_park_weather.csv weather_raw

create table weather AS SELECT
    usaf, wban, ts,
    CAST(NULLIF(DIR,'')   AS NUMERIC) wind_direction,
    CAST(NULLIF(SPD  ,'') AS NUMERIC) wind_speed,
    CAST(NULLIF(GUS,'')   AS NUMERIC) wind_gust,
    CAST(NULLIF(CLG,'')   AS NUMERIC) ceiling,
    NULLIF(SKC,'') SKC,
    CAST(NULLIF(VSB,'')  AS NUMERIC) visibility,
    CAST(NULLIF(TEMP,'') AS NUMERIC) temperature,
    CAST(NULLIF(DEWP,'') AS NUMERIC) dewpoint,
    CAST(NULLIF(SLP,'')  AS NUMERIC) slp,
    CAST(NULLIF(ALT,'')  AS NUMERIC) alt,
    CAST(NULLIF(STP,'')  AS NUMERIC) stp,
    CAST(NULLIF(MAXT,'') AS NUMERIC) max_temperature,
    CAST(NULLIF(MINT,'') AS NUMERIC) min_temperature,
    NULLIF(PCP01,'') precipitation_1_hour,
    NULLIF(PCP06,'') precipitiaton_6_hours,
    NULLIF(PCP24,'') precipitation_24_hours,
    NULLIF(PCPXX,'') precipitation_xx,
    NULLIF(SD,'') sd
FROM weather_raw;

drop table weather_raw;



