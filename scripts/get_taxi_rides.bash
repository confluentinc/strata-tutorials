#!/usr/bin/env bash

wget https://storage.googleapis.com/tlc-trip-data/2015/yellow_tripdata_2015-12.csv
awk /^[[:digit:]]/ yellow_tripdata_2015-12.csv > data/yellow_tripdata_2015-12.nohdr.csv
rm yellow_tripdata_2015-12.csv
cat src/sql/taxis.sql | sqlite3 data/cabsDec2015.db